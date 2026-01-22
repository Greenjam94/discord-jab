"""Competition tracking commands for Discord bot."""

import discord
from discord.ext import commands
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
import random
import asyncio
from torn_api import TornAPIClient, TornKeyManager
from torn_api.client import TornAPIError
from commands.competition_utils import (
    CONTRIBUTOR_STATS,
    get_status_emoji,
    check_database_available,
    validate_competition_exists,
    require_admin,
    find_faction_api_key,
    process_faction_members,
    format_number_with_sign,
    get_all_faction_keys,
    find_best_key_for_faction
)


async def _fetch_and_upsert_faction_data(
    bot: commands.Bot,
    client: TornAPIClient,
    key_value: str,
    faction_id: int
) -> Tuple[str, Dict[str, Any], List[int]]:
    """Fetch faction data and upsert to database.
    
    Args:
        bot: Bot instance
        client: TornAPIClient instance
        key_value: API key value
        faction_id: Faction ID to fetch
        
    Returns:
        Tuple of (faction_name, members_dict, list of member_ids)
    """
    # Fetch faction data with members
    faction_data = await client.get_faction(
        key_value,
        faction_id,
        selections=["basic"]
    )
    
    # Extract faction info and save
    faction_name = faction_data.get("name", "Unknown")
    tag = faction_data.get("tag")
    leader_id = faction_data.get("leader")
    co_leader_id = faction_data.get("co-leader")
    respect = faction_data.get("respect")
    age = faction_data.get("age")
    best_chain = faction_data.get("best_chain")
    members = faction_data.get("members", {})
    member_count = len(members) if isinstance(members, dict) else 0
    
    await bot.database.upsert_faction(
        faction_id=faction_id,
        name=faction_name,
        tag=tag if isinstance(tag, str) else None,
        leader_id=leader_id if isinstance(leader_id, int) else None,
        co_leader_id=co_leader_id if isinstance(co_leader_id, int) else None,
        respect=respect if isinstance(respect, int) else None,
        age=age if isinstance(age, int) else None,
        best_chain=best_chain if isinstance(best_chain, int) else None,
        member_count=member_count
    )
    
    # Process members and collect IDs
    if isinstance(members, dict):
        await process_faction_members(bot, members, faction_id)
        
        all_faction_member_ids = []
        for member_id_str in members.keys():
            try:
                member_id = int(member_id_str)
                all_faction_member_ids.append(member_id)
            except (ValueError, KeyError, TypeError) as e:
                print(f"Warning: Could not process member {member_id_str}: {e}")
                continue
    else:
        all_faction_member_ids = []
    
    return faction_name, members, all_faction_member_ids


async def _determine_players_to_add(
    player_ids: Optional[str],
    all_faction_member_ids: List[int],
    competition_id: int,
    bot: commands.Bot,
    interaction: discord.Interaction
) -> Optional[List[int]]:
    """Determine which players should be added to the competition.
    
    Args:
        player_ids: Optional comma-separated list of player IDs
        all_faction_member_ids: List of all faction member IDs
        competition_id: Competition ID
        bot: Bot instance
        interaction: Discord interaction
        
    Returns:
        List of player IDs to add, or None if error (error already sent)
    """
    # Determine which players to add
    if player_ids:
        # Parse provided player IDs
        try:
            requested_player_ids = [int(pid.strip()) for pid in player_ids.split(",")]
        except ValueError:
            await interaction.followup.send("❌ Invalid player IDs. Use comma-separated integers.", ephemeral=True)
            return None
        
        # Filter to only include players that are in the faction
        player_ids_to_add = [pid for pid in requested_player_ids if pid in all_faction_member_ids]
        
        if not player_ids_to_add:
            await interaction.followup.send(
                "❌ None of the provided player IDs are members of the specified faction.",
                ephemeral=True
            )
            return None
        
        # Warn if some requested IDs weren't in faction
        missing_ids = [pid for pid in requested_player_ids if pid not in all_faction_member_ids]
        if missing_ids:
            await interaction.followup.send(
                f"⚠️ Warning: {len(missing_ids)} player ID(s) not found in faction: {', '.join(map(str, missing_ids[:5]))}",
                ephemeral=True
            )
    else:
        # Add all faction members
        player_ids_to_add = all_faction_member_ids
    
    # Get existing participants and filter out already added players
    existing_participants = await bot.database.get_competition_participants(competition_id)
    existing_player_ids = {p["player_id"] for p in existing_participants}
    
    # Filter out already added players
    new_player_ids = [pid for pid in player_ids_to_add if pid not in existing_player_ids]
    
    if not new_player_ids:
        await interaction.followup.send("❌ All selected players are already participants.", ephemeral=True)
        return None
    
    return new_player_ids


async def _assign_and_add_participants(
    bot: commands.Bot,
    competition_id: int,
    comp: Dict[str, Any],
    new_player_ids: List[int]
) -> Tuple[int, List[Dict[str, Any]]]:
    """Assign participants to teams and add them to competition.
    
    Args:
        bot: Bot instance
        competition_id: Competition ID
        comp: Competition dict
        new_player_ids: List of player IDs to add
        
    Returns:
        Tuple of (assigned_count, failed_assignments list)
    """
    # Get teams (nullable - competitions might not have teams yet)
    teams = await bot.database.get_competition_teams(competition_id)
    team_ids = [t["id"] for t in teams] if teams else []
    
    # Randomly assign to teams (round-robin style for even distribution)
    random.shuffle(new_player_ids)  # Shuffle for randomness
    assigned_count = 0
    failed_assignments = []
    
    # Get current stat values for start stats
    now_utc = int(datetime.utcnow().timestamp())
    for i, player_id in enumerate(new_player_ids):
        # Assign to team if teams exist, otherwise None
        team_id = team_ids[i % len(team_ids)] if team_ids else None
        
        try:
            # Add participant
            await bot.database.add_competition_participant(
                competition_id=competition_id,
                player_id=player_id,
                team_id=team_id,
                discord_user_id=None  # Can be linked later
            )
            
            # Set start stat if competition has started
            if now_utc >= comp["start_date"]:
                current_stat = await bot.database.get_player_current_stat_value(
                    player_id, comp["tracked_stat"]
                )
                if current_stat is not None:
                    await bot.database.set_competition_start_stat(
                        comp["id"], player_id, current_stat, stat_source="contributors"
                    )
            
            assigned_count += 1
            
        except Exception as e:
            failed_assignments.append({"player_id": player_id, "error": str(e)})
            print(f"Error adding participant {player_id}: {e}")
            continue
    
    return assigned_count, failed_assignments


def setup(bot: commands.Bot):
    """Setup function to register competition commands."""
    
    @bot.tree.command(name="competition-list", description="List all competitions")
    @discord.app_commands.describe(
        status="Filter by status: active, cancelled, or completed"
    )
    async def competition_list(interaction: discord.Interaction, status: Optional[str] = None):
        """List all competitions."""
        await interaction.response.defer(ephemeral=True)
        
        if not await check_database_available(bot, interaction):
            return
        
        try:
            valid_statuses = ["active", "cancelled", "completed"]
            if status and status.lower() not in valid_statuses:
                await interaction.followup.send(
                    f"❌ Invalid status. Must be one of: {', '.join(valid_statuses)}",
                    ephemeral=True
                )
                return
            
            competitions = await bot.database.list_competitions(status=status.lower() if status else None)
            
            if not competitions:
                status_msg = f" with status '{status}'" if status else ""
                await interaction.followup.send(f"No competitions found{status_msg}.", ephemeral=True)
                return
            
            embed = discord.Embed(
                title="Competitions List",
                color=discord.Color.blue()
            )
            
            for comp in competitions[:10]:  # Show max 10
                status_emoji = get_status_emoji(comp["status"])
                
                start_date = datetime.fromtimestamp(comp["start_date"]).strftime("%Y-%m-%d")
                end_date = datetime.fromtimestamp(comp["end_date"]).strftime("%Y-%m-%d")
                
                embed.add_field(
                    name=f"{status_emoji} {comp['name']}",
                    value=(
                        f"**Stat:** {comp['tracked_stat']}\n"
                        f"**Period:** {start_date} to {end_date}\n"
                        f"**Status:** {comp['status']}\n"
                        f"**ID:** {comp['id']}"
                    ),
                    inline=False
                )
            
            if len(competitions) > 10:
                embed.set_footer(text=f"Showing 10 of {len(competitions)} competitions")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
    
    @bot.tree.command(name="competition-create", description="Create a new competition")
    @discord.app_commands.describe(
        name="Name for the competition",
        tracked_stat="Stat to track (contributor stat name, e.g., gymstrength, gym_e_spent, revives, etc.)",
        start_date="Start date (YYYY-MM-DD)",
        end_date="End date (YYYY-MM-DD)",
        num_teams="Number of teams (default: 4)"
    )
    async def competition_create(
        interaction: discord.Interaction,
        name: str,
        tracked_stat: str,
        start_date: str,
        end_date: str,
        num_teams: int = 4
    ):
        """Create a new competition."""
        # Check permissions (if set in database, otherwise allow everyone)
        try:
            from utils.permissions import check_command_permission, get_bot_database
            db = get_bot_database(bot)
            if db:
                has_permission, error_msg = await check_command_permission(interaction, "competition-create", db)
                if not has_permission:
                    await interaction.response.send_message(
                        error_msg or "You don't have permission to use this command.",
                        ephemeral=True
                    )
                    return
        except Exception:
            pass  # If permission check fails, allow command (fail open)
        
        await interaction.response.defer(ephemeral=True)
        
        if not await check_database_available(bot, interaction):
            return
        
        try:
            # Validate stat name
            if tracked_stat.lower() not in CONTRIBUTOR_STATS:
                await interaction.followup.send(
                    f"❌ Invalid stat. Must be one of: {', '.join(CONTRIBUTOR_STATS)}",
                    ephemeral=True
                )
                return
            
            # Parse dates (UTC)
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                await interaction.followup.send(
                    "❌ Invalid date format. Use YYYY-MM-DD",
                    ephemeral=True
                )
                return
            
            # Ensure dates are at start/end of day in UTC
            start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=0)
            
            # Validate dates (must be current day or future)
            now_utc = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            if start_dt < now_utc:
                await interaction.followup.send(
                    "❌ Start date must be today or in the future.",
                    ephemeral=True
                )
                return
            
            if end_dt <= start_dt:
                await interaction.followup.send(
                    "❌ End date must be after start date.",
                    ephemeral=True
                )
                return
            
            # Validate team count
            if num_teams < 2 or num_teams > 20:
                await interaction.followup.send(
                    "❌ Number of teams must be between 2 and 20.",
                    ephemeral=True
                )
                return
            
            # Convert to timestamps
            start_timestamp = int(start_dt.timestamp())
            end_timestamp = int(end_dt.timestamp())
            
            # Create competition
            competition_id = await bot.database.create_competition(
                name=name,
                tracked_stat=tracked_stat.lower(),
                start_date=start_timestamp,
                end_date=end_timestamp,
                created_by=str(interaction.user.id)
            )
            
            # Create teams
            team_ids = []
            for i in range(1, num_teams + 1):
                team_id = await bot.database.create_competition_team(
                    competition_id=competition_id,
                    team_name=f"Team {i}",
                    captain_discord_id_1="",  # Will be set later
                    captain_discord_id_2=None
                )
                team_ids.append(team_id)
            
            embed = discord.Embed(
                title="✅ Competition Created",
                color=discord.Color.green(),
                description=f"Competition **{name}** has been created."
            )
            embed.add_field(name="Competition ID", value=str(competition_id), inline=True)
            embed.add_field(name="Tracked Stat", value=tracked_stat.lower(), inline=True)
            embed.add_field(name="Start Date", value=start_date, inline=True)
            embed.add_field(name="End Date", value=end_date, inline=True)
            embed.add_field(name="Teams", value=str(num_teams), inline=True)
            embed.add_field(
                name="Next Steps",
                value=(
                    "1. Set team captains using `/competition-team-set-captains`\n"
                    "2. Add participants using `/competition-add-participants`\n"
                    "3. Participants will be randomly assigned to teams"
                ),
                inline=False
            )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()
    
    @bot.tree.command(name="competition-cancel", description="Cancel a competition (Admin only)")
    @discord.app_commands.describe(
        competition_id="ID of the competition to cancel"
    )
    async def competition_cancel(interaction: discord.Interaction, competition_id: int):
        """Cancel a competition."""
        # Check admin permission (checks database permissions first)
        if not await require_admin(interaction, "competition-cancel", bot):
            return
        
        await interaction.response.defer(ephemeral=True)
        
        if not await check_database_available(bot, interaction):
            return
        
        try:
            comp = await validate_competition_exists(bot, competition_id, interaction)
            if not comp:
                return
            
            if comp["status"] == "cancelled":
                await interaction.followup.send("❌ Competition is already cancelled.", ephemeral=True)
                return
            
            await bot.database.cancel_competition(competition_id)
            
            embed = discord.Embed(
                title="✅ Competition Cancelled",
                color=discord.Color.orange(),
                description=f"Competition **{comp['name']}** has been cancelled."
            )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
    @bot.tree.command(name="competition-status", description="Get competition status and current rankings")
    @discord.app_commands.describe(
        competition_id="ID of the competition",
        show_worst="Show worst ranks instead of best (default: False)",
        limit="Number of results to show (default: 10 or all if less)"
    )
    async def competition_status(
        interaction: discord.Interaction,
        competition_id: int,
        show_worst: bool = False,
        limit: Optional[int] = None
    ):
        """Get competition status and current rankings."""
        await interaction.response.defer(ephemeral=True)
        
        if not await check_database_available(bot, interaction):
            return
        
        try:
            comp = await validate_competition_exists(bot, competition_id, interaction)
            if not comp:
                return
            
            participants = await bot.database.get_competition_participants(competition_id)
            
            if not participants:
                await interaction.followup.send("❌ No participants found for this competition.", ephemeral=True)
                return
            
            # Calculate rankings
            rankings = []
            for participant in participants:
                player_id = participant["player_id"]
                
                # Get start stat
                start_stat = await bot.database.get_competition_start_stat(competition_id, player_id)
                
                # Get current stat
                current_stat = await bot.database.get_player_current_stat_value(
                    player_id, comp["tracked_stat"]
                )
                
                # Calculate delta
                delta = None
                if start_stat is not None and current_stat is not None:
                    delta = current_stat - start_stat
                elif start_stat is None:
                    # Use current as start if not set yet
                    delta = 0.0
                
                rankings.append({
                    "player_id": player_id,
                    "player_name": participant["player_name"] or f"Player {player_id}",
                    "team_id": participant["team_id"],
                    "start_stat": start_stat,
                    "current_stat": current_stat,
                    "delta": delta
                })
            
            # Sort by delta (best first, unless show_worst)
            rankings.sort(key=lambda x: x["delta"] if x["delta"] is not None else float('-inf'), reverse=not show_worst)
            
            # Determine how many to show (all or top 10, whichever is less)
            max_results = limit if limit else min(10, len(rankings))
            display_rankings = rankings[:max_results]
            
            # Build embed
            status_emoji = get_status_emoji(comp["status"])
            
            embed = discord.Embed(
                title=f"{status_emoji} Competition: {comp['name']}",
                color=discord.Color.blue(),
                description=f"**Tracked Stat:** {comp['tracked_stat']}"
            )
            
            start_date_str = datetime.fromtimestamp(comp["start_date"]).strftime("%Y-%m-%d")
            end_date_str = datetime.fromtimestamp(comp["end_date"]).strftime("%Y-%m-%d")
            embed.add_field(name="Period", value=f"{start_date_str} to {end_date_str}", inline=False)
            
            # Build rankings text
            rank_type = "Worst" if show_worst else "Top"
            rankings_text = []
            
            for i, rank in enumerate(display_rankings, 1):
                delta_str = format_number_with_sign(rank['delta'])
                current_str = f"{rank['current_stat']:,.0f}" if rank['current_stat'] is not None else "N/A"
                
                rankings_text.append(
                    f"**{i}.** {rank['player_name']} [{rank['player_id']}]\n"
                    f"   Change: {delta_str} | Current: {current_str}"
                )
            
            if rankings_text:
                embed.add_field(
                    name=f"{rank_type} {len(display_rankings)} Rankings",
                    value="\n".join(rankings_text),
                    inline=False
                )
            else:
                embed.add_field(name="Rankings", value="No data available yet.", inline=False)
            
            if len(rankings) > max_results:
                embed.set_footer(text=f"Showing {max_results} of {len(rankings)} participants")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()
    
    @bot.tree.command(name="competition-team-status", description="Get team rankings for a competition")
    @discord.app_commands.describe(
        competition_id="ID of the competition",
        team_id="Team ID (optional, to show specific team)",
        show_worst="Show worst teams instead of best (default: False)"
    )
    async def competition_team_status(
        interaction: discord.Interaction,
        competition_id: int,
        team_id: Optional[int] = None,
        show_worst: bool = False
    ):
        """Get team rankings for a competition."""
        await interaction.response.defer(ephemeral=True)
        
        if not await check_database_available(bot, interaction):
            return
        
        try:
            comp = await validate_competition_exists(bot, competition_id, interaction)
            if not comp:
                return
            
            # Get teams
            teams = await bot.database.get_competition_teams(competition_id)
            
            # Filter by team_id if provided
            if team_id:
                teams = [t for t in teams if t["id"] == team_id]
                if not teams:
                    await interaction.followup.send(f"❌ Team with ID {team_id} not found.", ephemeral=True)
                    return
            
            # Get participants
            participants = await bot.database.get_competition_participants(competition_id)
            
            # Calculate team totals
            team_totals = {}
            for team in teams:
                team_totals[team["id"]] = {
                    "team_name": team["team_name"],
                    "captain_1": team["captain_discord_id_1"],
                    "captain_2": team["captain_discord_id_2"],
                    "total_delta": 0.0,
                    "participant_count": 0
                }
            
            # Check if user is admin or captain
            is_admin = interaction.user.guild_permissions.administrator
            user_discord_id = str(interaction.user.id)
            is_captain = False
            
            if team_id:
                team = teams[0]
                is_captain = (
                    team["captain_discord_id_1"] == user_discord_id or
                    team["captain_discord_id_2"] == user_discord_id
                )
            
            # If specific team requested, check permissions
            if team_id and not (is_admin or is_captain):
                await interaction.followup.send(
                    "❌ You must be an admin or team captain to view individual team status.",
                    ephemeral=True
                )
                return
            
            # Calculate deltas for each participant and sum by team
            for participant in participants:
                if not participant["team_id"]:
                    continue
                
                team_id_key = participant["team_id"]
                if team_id_key not in team_totals:
                    continue
                
                player_id = participant["player_id"]
                start_stat = await bot.database.get_competition_start_stat(competition_id, player_id)
                current_stat = await bot.database.get_player_current_stat_value(
                    player_id, comp["tracked_stat"]
                )
                
                delta = 0.0
                if start_stat is not None and current_stat is not None:
                    delta = current_stat - start_stat
                
                team_totals[team_id_key]["total_delta"] += delta
                team_totals[team_id_key]["participant_count"] += 1
            
            # Convert to list and sort
            team_rankings = list(team_totals.values())
            team_rankings.sort(key=lambda x: x["total_delta"], reverse=not show_worst)
            
            # Build embed
            status_emoji = get_status_emoji(comp["status"])
            
            embed = discord.Embed(
                title=f"{status_emoji} Team Rankings: {comp['name']}",
                color=discord.Color.blue(),
                description=f"**Tracked Stat:** {comp['tracked_stat']}"
            )
            
            rank_type = "Worst" if show_worst else "Top"
            rankings_text = []
            
            for i, team in enumerate(team_rankings, 1):
                delta_str = format_number_with_sign(team['total_delta'])
                rankings_text.append(
                    f"**{i}.** {team['team_name']}\n"
                    f"   Total Change: {delta_str} | Members: {team['participant_count']}"
                )
            
            if rankings_text:
                embed.add_field(
                    name=f"{rank_type} Team Rankings",
                    value="\n".join(rankings_text),
                    inline=False
                )
            else:
                embed.add_field(name="Team Rankings", value="No team data available.", inline=False)
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()
    
    @bot.tree.command(name="competition-faction-overview", description="Get overall faction improvement summary")
    @discord.app_commands.describe(
        competition_id="ID of the competition"
    )
    async def competition_faction_overview(
        interaction: discord.Interaction,
        competition_id: int
    ):
        """Get overall faction improvement summary (regardless of teams)."""
        await interaction.response.defer(ephemeral=True)
        
        if not await check_database_available(bot, interaction):
            return
        
        try:
            comp = await validate_competition_exists(bot, competition_id, interaction)
            if not comp:
                return
            
            participants = await bot.database.get_competition_participants(competition_id)
            
            if not participants:
                await interaction.followup.send("❌ No participants found for this competition.", ephemeral=True)
                return
            
            # Calculate totals
            total_delta = 0.0
            participant_count = 0
            participants_with_data = 0
            
            for participant in participants:
                player_id = participant["player_id"]
                start_stat = await bot.database.get_competition_start_stat(competition_id, player_id)
                current_stat = await bot.database.get_player_current_stat_value(
                    player_id, comp["tracked_stat"]
                )
                
                if start_stat is not None and current_stat is not None:
                    delta = current_stat - start_stat
                    total_delta += delta
                    participants_with_data += 1
                
                participant_count += 1
            
            # Build embed
            status_emoji = get_status_emoji(comp["status"])
            
            embed = discord.Embed(
                title=f"{status_emoji} Faction Overview: {comp['name']}",
                color=discord.Color.green(),
                description=f"**Tracked Stat:** {comp['tracked_stat']}"
            )
            
            total_delta_str = format_number_with_sign(total_delta)
            avg_delta = total_delta / participants_with_data if participants_with_data > 0 else 0.0
            avg_delta_str = format_number_with_sign(avg_delta)
            
            embed.add_field(name="Total Improvement", value=total_delta_str, inline=True)
            embed.add_field(name="Average per Participant", value=avg_delta_str, inline=True)
            embed.add_field(name="Participants", value=f"{participant_count} total\n{participants_with_data} with data", inline=True)
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()

    @bot.tree.command(name="competition-team-set-captains", description="Set captains for a competition team (Admin only)")
    @discord.app_commands.describe(
        competition_id="ID of the competition",
        team_id="ID of the team",
        captain_1="Discord user ID for first captain",
        captain_2="Discord user ID for second captain (optional)"
    )
    async def competition_team_set_captains(
        interaction: discord.Interaction,
        competition_id: int,
        team_id: int,
        captain_1: str,
        captain_2: Optional[str] = None
    ):
        """Set team captains."""
        if not await require_admin(interaction, "competition-team-set-captains", bot):
            return
        
        await interaction.response.defer(ephemeral=True)
        
        if not await check_database_available(bot, interaction):
            return
        
        try:
            comp = await validate_competition_exists(bot, competition_id, interaction)
            if not comp:
                return
            
            teams = await bot.database.get_competition_teams(competition_id)
            if not any(t["id"] == team_id for t in teams):
                await interaction.followup.send(f"❌ Team with ID {team_id} not found in this competition.", ephemeral=True)
                return
            
            await bot.database.update_team_captains(team_id, captain_1, captain_2)
            
            team_name = next((t["team_name"] for t in teams if t["id"] == team_id), "Unknown")
            embed = discord.Embed(
                title="✅ Team Captains Updated",
                color=discord.Color.green(),
                description=f"Captains set for **{team_name}**"
            )
            embed.add_field(name="Captain 1", value=f"<@{captain_1}>", inline=True)
            if captain_2:
                embed.add_field(name="Captain 2", value=f"<@{captain_2}>", inline=True)
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()
    
    @bot.tree.command(name="competition-add-participants", description="Add participants from a faction to a competition and randomly assign to teams (Admin only)")
    @discord.app_commands.describe(
        competition_id="ID of the competition",
        faction_id="Faction ID (leave blank to use key owner's faction)",
        player_ids="Optional: Comma-separated list of specific Torn player IDs to add (if not provided, adds all faction members)"
    )
    async def competition_add_participants(
        interaction: discord.Interaction,
        competition_id: int,
        faction_id: Optional[int] = None,
        player_ids: Optional[str] = None
    ):
        """Add participants from a faction and randomly assign them to teams."""
        if not await require_admin(interaction, "competition-add-participants", bot):
            return
        
        await interaction.response.defer(ephemeral=True)
        
        if not await check_database_available(bot, interaction):
            return
        
        try:
            comp = await validate_competition_exists(bot, competition_id, interaction)
            if not comp:
                return
            
            # Get teams
            teams = await bot.database.get_competition_teams(competition_id)
            if not teams:
                await interaction.followup.send("❌ No teams found for this competition.", ephemeral=True)
                return
            
            # Initialize API client
            key_manager = TornKeyManager()
            client = TornAPIClient()
            
            # Find API key with faction permission
            key_result = await find_faction_api_key(key_manager, faction_id, client)
            if not key_result:
                await interaction.followup.send(
                    "❌ No API key found with faction permission. Please register a key with faction access using `/torn-key-add`.",
                    ephemeral=True
                )
                return
            
            key_alias, key_value = key_result
            
            try:
                # Determine faction_id
                if faction_id is None:
                    # Get key owner's faction
                    owner_data = await client.get_user(key_value, None, selections=["basic"])
                    faction_id = owner_data.get("faction", {}).get("faction_id") if isinstance(owner_data.get("faction"), dict) else owner_data.get("faction")
                    
                    if not faction_id:
                        await interaction.followup.send(
                            "❌ Key owner is not in a faction. Please specify a faction_id.",
                            ephemeral=True
                        )
                        return
                
                # Fetch and upsert faction data
                faction_name, members, all_faction_member_ids = await _fetch_and_upsert_faction_data(
                    bot, client, key_value, faction_id
                )
                member_count = len(all_faction_member_ids)
                
                # Determine which players to add
                new_player_ids = await _determine_players_to_add(
                    player_ids, all_faction_member_ids, competition_id, bot, interaction
                )
                if new_player_ids is None:
                    return
                
                # Assign and add participants
                assigned_count, failed_assignments = await _assign_and_add_participants(
                    bot, competition_id, comp, new_player_ids
                )
                
                # Build response
                embed = discord.Embed(
                    title="✅ Participants Added",
                    color=discord.Color.green(),
                    description=f"Added participants from **{faction_name}** to **{comp['name']}**"
                )
                
                embed.add_field(name="Faction", value=f"{faction_name} ({faction_id})", inline=False)
                embed.add_field(name="Added", value=str(assigned_count), inline=True)
                embed.add_field(name="Total Faction Members", value=str(member_count), inline=True)
                
                if failed_assignments:
                    embed.add_field(name="Failed", value=str(len(failed_assignments)), inline=True)
                    embed.color = discord.Color.orange()
                    
                    failure_details = []
                    for fail in failed_assignments[:5]:  # Show first 5
                        failure_details.append(f"Player {fail['player_id']}: {fail['error']}")
                    embed.add_field(
                        name="⚠️ Failed Assignments",
                        value="\n".join(failure_details),
                        inline=False
                    )
                
                await interaction.followup.send(embed=embed, ephemeral=True)
                
            except TornAPIError as api_error:
                error_code = getattr(api_error, 'error_code', None)
                error_msg = str(api_error)
                
                if error_code == 7 or "permission" in error_msg.lower():
                    await interaction.followup.send(
                        f"❌ Permission denied: Cannot access faction {faction_id}. The API key may not have access to this faction.",
                        ephemeral=True
                    )
                elif error_code == 5:
                    await interaction.followup.send(
                        "❌ Rate limit exceeded. Please try again in a minute.",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        f"❌ API Error: {error_msg}",
                        ephemeral=True
                    )
            finally:
                await client.close()
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()
    
    @bot.tree.command(name="competition-update-assignment", description="Update a participant's team assignment (Admin only)")
    @discord.app_commands.describe(
        competition_id="ID of the competition",
        player_id="Torn player ID",
        team_id="New team ID (use 0 to remove from team)"
    )
    async def competition_update_assignment(
        interaction: discord.Interaction,
        competition_id: int,
        player_id: int,
        team_id: int
    ):
        """Update a participant's team assignment."""
        if not await require_admin(interaction, "competition-update-assignment", bot):
            return
        
        await interaction.response.defer(ephemeral=True)
        
        if not await check_database_available(bot, interaction):
            return
        
        try:
            comp = await validate_competition_exists(bot, competition_id, interaction)
            if not comp:
                return
            
            participants = await bot.database.get_competition_participants(competition_id)
            participant = next((p for p in participants if p["player_id"] == player_id), None)
            
            if not participant:
                await interaction.followup.send(f"❌ Player {player_id} is not a participant in this competition.", ephemeral=True)
                return
            
            # Use None for team_id if 0 is provided (remove from team)
            new_team_id = None if team_id == 0 else team_id
            
            if new_team_id:
                teams = await bot.database.get_competition_teams(competition_id)
                if not any(t["id"] == new_team_id for t in teams):
                    await interaction.followup.send(f"❌ Team with ID {team_id} not found in this competition.", ephemeral=True)
                    return
            
            await bot.database.update_participant_team(competition_id, player_id, new_team_id)
            
            team_status = f"Team {new_team_id}" if new_team_id else "No team"
            embed = discord.Embed(
                title="✅ Assignment Updated",
                color=discord.Color.green(),
                description=f"Player {player_id} assigned to {team_status}"
            )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()
    
    @bot.tree.command(name="competition-update-stats", description="Manually trigger stat update for active competitions (Admin only)")
    async def competition_update_stats(interaction: discord.Interaction):
        """Manually trigger stat updates for all active competitions."""
        if not await require_admin(interaction, "competition-update-stats", bot):
            return
        
        await interaction.response.defer(ephemeral=True)
        
        if not await check_database_available(bot, interaction):
            return
        
        try:
            # This will trigger the same update function used by the scheduled task
            # We'll implement the actual update logic in a shared function
            # For now, just acknowledge the command
            embed = discord.Embed(
                title="⏳ Stats Update Started",
                color=discord.Color.blue(),
                description="Updating stats for all active competitions. This may take a moment..."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            
            # Call the update function
            result = await _update_competition_stats_helper(bot)
            
            result_embed = discord.Embed(
                title="✅ Stats Update Complete",
                color=discord.Color.green(),
                description=result.get("message", "Stats updated successfully")
            )
            competitions_updated_count = result.get("competitions_updated", 0)
            participants_updated_count = result.get("participants_updated", 0)
            participants_failed_count = result.get("participants_failed_count", 0)
            factions_processed = result.get("factions_processed", 0)
            
            result_embed.add_field(name="Competitions", value=str(competitions_updated_count), inline=True)
            result_embed.add_field(name="Participants Updated", value=str(participants_updated_count), inline=True)
            result_embed.add_field(name="Factions Processed", value=str(factions_processed), inline=True)
            
            # Show failed factions
            failed_factions = result.get("factions_failed", [])
            if failed_factions:
                faction_failures = []
                for faction_fail in failed_factions[:5]:  # Show first 5
                    faction_id = faction_fail.get("faction_id", "Unknown")
                    reason = faction_fail.get("reason", "Unknown error")
                    participant_count = faction_fail.get("participant_count", 0)
                    faction_failures.append(f"**Faction {faction_id}** ({participant_count} participants)\n{reason}")
                
                result_embed.add_field(
                    name="⚠️ Failed Factions",
                    value="\n\n".join(faction_failures),
                    inline=False
                )
                if len(failed_factions) > 5:
                    result_embed.set_footer(text=f"And {len(failed_factions) - 5} more failed factions")
            
            # Show user intervention needed
            user_intervention = result.get("user_intervention_needed", [])
            if user_intervention:
                intervention_list = []
                for faction_fail in user_intervention[:3]:  # Show first 3
                    faction_id = faction_fail.get("faction_id", "Unknown")
                    participant_count = faction_fail.get("participant_count", 0)
                    intervention_list.append(f"Faction {faction_id} ({participant_count} participants)")
                
                result_embed.add_field(
                    name="🔧 User Intervention Needed",
                    value=(
                        "The following factions need new API keys with faction permission:\n" +
                        "\n".join(intervention_list) +
                        "\n\nPlease add a new API key or manually refresh stats."
                    ),
                    inline=False
                )
            
            # Add failed participants details if any
            failed_participants = result.get("participants_failed", [])
            if failed_participants:
                # Group by reason for cleaner display
                reasons = {}
                for fail in failed_participants:
                    reason = fail.get("reason", "Unknown error")
                    if reason not in reasons:
                        reasons[reason] = []
                    reasons[reason].append(fail["player_id"])
                
                failure_details = []
                for reason, player_ids in reasons.items():
                    if len(player_ids) <= 3:
                        ids_str = ", ".join(map(str, player_ids))
                    else:
                        ids_str = f"{', '.join(map(str, player_ids[:3]))} and {len(player_ids) - 3} more"
                    failure_details.append(f"**{reason}**\nPlayers: {ids_str}")
                
                result_embed.add_field(
                    name="⚠️ Failed Participants",
                    value="\n\n".join(failure_details[:3]),  # Show first 3 failure types
                    inline=False
                )
            
            # Set embed color based on results
            if participants_updated_count > 0 and len(failed_factions) == 0:
                result_embed.color = discord.Color.green()
            elif participants_updated_count > 0:
                result_embed.color = discord.Color.orange()
            else:
                result_embed.color = discord.Color.red()
            
            await interaction.followup.send(embed=result_embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()
    
    @bot.tree.command(name="competition-progress", description="Show competition progress graph (visible to channel)")
    @discord.app_commands.describe(
        competition_id="ID of the competition",
        view_type="View type: individual or team (default: team)",
        limit="Number of top entries to show (default: 10)"
    )
    async def competition_progress(
        interaction: discord.Interaction,
        competition_id: int,
        view_type: Optional[str] = "team",
        limit: Optional[int] = 10
    ):
        """Show competition progress as a visual graph in Discord embed."""
        await interaction.response.defer(ephemeral=False)  # Visible to channel
        
        if not await check_database_available(bot, interaction):
            return
        
        try:
            comp = await validate_competition_exists(bot, competition_id, interaction)
            if not comp:
                return
            
            view_type = view_type.lower() if view_type else "team"
            if view_type not in ["individual", "team"]:
                await interaction.followup.send("❌ View type must be 'individual' or 'team'.", ephemeral=True)
                return
            
            # Get progress data
            progress_data = await _get_competition_progress_data(
                bot, competition_id, view_type
            )
            
            if 'error' in progress_data:
                await interaction.followup.send(f"❌ {progress_data['error']}", ephemeral=True)
                return
            
            # Build embed
            status_emoji = get_status_emoji(comp["status"])
            embed = discord.Embed(
                title=f"{status_emoji} Competition Progress: {comp['name']}",
                color=discord.Color.blue(),
                description=f"**Tracked Stat:** `{comp['tracked_stat']}`\n**View:** {view_type.title()}"
            )
            
            # Add competition period
            start_date_str = datetime.fromtimestamp(comp["start_date"]).strftime("%Y-%m-%d")
            end_date_str = datetime.fromtimestamp(comp["end_date"]).strftime("%Y-%m-%d")
            embed.add_field(name="Period", value=f"{start_date_str} to {end_date_str}", inline=False)
            
            # Get top entries
            table_data = progress_data.get('table_data', [])
            if not table_data:
                embed.add_field(
                    name="No Data",
                    value="No progress data available yet. Stats will appear after the first update.",
                    inline=False
                )
                await interaction.followup.send(embed=embed)
                return
            
            # Limit entries
            display_data = table_data[:limit] if limit else table_data
            
            # Create progress visualization
            if view_type == "team":
                # Team view - show progress bars
                progress_text = _create_progress_bars(display_data, "team")
                # Discord field value limit is 1024 characters
                if len(progress_text) > 1024:
                    progress_text = progress_text[:1021] + "..."
                embed.add_field(
                    name=f"🏆 Top {len(display_data)} Teams",
                    value=progress_text,
                    inline=False
                )
            else:
                # Individual view - show progress bars
                progress_text = _create_progress_bars(display_data, "individual")
                # Discord field value limit is 1024 characters
                if len(progress_text) > 1024:
                    progress_text = progress_text[:1021] + "..."
                embed.add_field(
                    name=f"👤 Top {len(display_data)} Participants",
                    value=progress_text,
                    inline=False
                )
            
            # Add summary stats
            if table_data:
                max_progress = max(row['latest_progress'] for row in table_data)
                min_progress = min(row['latest_progress'] for row in table_data)
                avg_progress = sum(row['latest_progress'] for row in table_data) / len(table_data)
                
                embed.add_field(
                    name="📊 Statistics",
                    value=(
                        f"**Max:** {format_number_with_sign(max_progress)}\n"
                        f"**Min:** {format_number_with_sign(min_progress)}\n"
                        f"**Avg:** {format_number_with_sign(avg_progress)}\n"
                        f"**Total Entries:** {len(table_data)}"
                    ),
                    inline=True
                )
            
            # Add note about data points
            total_data_points = sum(row.get('data_points', 0) for row in table_data)
            if total_data_points > 0:
                embed.set_footer(text=f"Total data points: {total_data_points} | Use /competition-update-stats to refresh")
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()


async def _get_competition_progress_data(
    bot: commands.Bot,
    competition_id: int,
    view_type: str = 'team'
) -> Dict[str, Any]:
    """Get competition progress data for Discord display.
    
    Similar to web app but simplified for Discord embeds.
    """
    comp = await bot.database.get_competition(competition_id)
    if not comp:
        return {'error': 'Competition not found'}
    
    tracked_stat = comp['tracked_stat']
    start_date = comp['start_date']
    end_date = comp['end_date']
    
    # Get participants
    participants = await bot.database.get_competition_participants(competition_id)
    if not participants:
        return {'error': 'No participants found'}
    
    # Get teams
    teams = await bot.database.get_competition_teams(competition_id)
    team_map = {t['id']: t['team_name'] for t in teams}
    
    # Get start stats
    start_stats = {}
    for participant in participants:
        player_id = participant['player_id']
        start_stat = await bot.database.get_competition_start_stat(competition_id, player_id)
        start_stats[player_id] = start_stat if start_stat is not None else 0.0
    
    # Get historical data
    if tracked_stat == "gym_e_spent":
        stat_names = ["gymstrength", "gymdefense", "gymspeed", "gymdexterity"]
    else:
        stat_names = [tracked_stat]
    
    # Get latest values from history
    participant_ids = [p['player_id'] for p in participants]
    history_data = {}
    
    for player_id in participant_ids:
        for stat_name in stat_names:
            # Get latest value from contributor history
            async with bot.database.connection.execute("""
                SELECT value FROM player_contributor_history
                WHERE player_id = ? AND stat_name = ?
                AND timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp DESC LIMIT 1
            """, (player_id, stat_name, start_date, end_date)) as cursor:
                row = await cursor.fetchone()
                if row:
                    if player_id not in history_data:
                        history_data[player_id] = {}
                    if stat_name not in history_data[player_id]:
                        history_data[player_id][stat_name] = 0.0
                    history_data[player_id][stat_name] += row[0]
    
    # Process data
    table_data = []
    
    if view_type == 'team':
        # Aggregate by team
        team_data = {}
        for participant in participants:
            player_id = participant['player_id']
            team_id_p = participant.get('team_id')
            start_stat = start_stats.get(player_id, 0.0)
            
            # Get current value
            current_value = 0.0
            if player_id in history_data:
                if tracked_stat == "gym_e_spent":
                    current_value = sum(history_data[player_id].values())
                else:
                    current_value = history_data[player_id].get(tracked_stat, start_stat)
            else:
                # Fallback to current stat value
                current_stat = await bot.database.get_player_current_stat_value(
                    player_id, tracked_stat
                )
                if current_stat is not None:
                    current_value = current_stat
            
            progress = current_value - start_stat
            
            if team_id_p not in team_data:
                team_data[team_id_p] = {
                    'name': team_map.get(team_id_p, f"Team {team_id_p}") if team_id_p else "No Team",
                    'progress': 0.0,
                    'count': 0
                }
            team_data[team_id_p]['progress'] += progress
            team_data[team_id_p]['count'] += 1
        
        for team_id_p, data in team_data.items():
            table_data.append({
                'name': data['name'],
                'latest_progress': data['progress'],
                'data_points': data['count']
            })
    else:
        # Individual view
        for participant in participants:
            player_id = participant['player_id']
            player_name = participant.get('player_name') or f"Player {player_id}"
            start_stat = start_stats.get(player_id, 0.0)
            
            # Get current value
            current_value = 0.0
            if player_id in history_data:
                if tracked_stat == "gym_e_spent":
                    current_value = sum(history_data[player_id].values())
                else:
                    current_value = history_data[player_id].get(tracked_stat, start_stat)
            else:
                # Fallback to current stat value
                current_stat = await bot.database.get_player_current_stat_value(
                    player_id, tracked_stat
                )
                if current_stat is not None:
                    current_value = current_stat
            
            progress = current_value - start_stat
            team_name = team_map.get(participant.get('team_id'), 'No Team') if participant.get('team_id') else 'No Team'
            
            table_data.append({
                'name': player_name,
                'player_id': player_id,
                'team': team_name,
                'latest_progress': progress,
                'data_points': 1
            })
    
    # Sort by progress
    table_data.sort(key=lambda x: x['latest_progress'], reverse=True)
    
    return {
        'competition': comp,
        'view_type': view_type,
        'table_data': table_data
    }


def _create_progress_bars(data: List[Dict[str, Any]], view_type: str) -> str:
    """Create text-based progress bars for Discord embed."""
    if not data:
        return "No data available"
    
    # Find max absolute value for scaling
    max_abs = max(abs(row['latest_progress']) for row in data)
    if max_abs == 0:
        max_abs = 1  # Avoid division by zero
    
    lines = []
    bar_length = 20  # Characters for progress bar
    
    for i, row in enumerate(data, 1):
        progress = row['latest_progress']
        name = row['name']
        
        # Truncate long names
        if len(name) > 25:
            name = name[:22] + "..."
        
        # Calculate bar fill
        fill_ratio = abs(progress) / max_abs
        fill_length = int(fill_ratio * bar_length)
        bar_fill = "█" * fill_length
        bar_empty = "░" * (bar_length - fill_length)
        
        # Choose emoji based on sign
        if progress >= 0:
            emoji = "📈"
            bar = bar_fill + bar_empty
        else:
            emoji = "📉"
            bar = bar_empty + bar_fill
        
        # Format value
        value_str = format_number_with_sign(progress)
        
        # Add team info for individual view
        if view_type == "individual" and 'team' in row:
            team_info = f" [{row['team']}]"
        else:
            team_info = ""
        
        lines.append(f"{i}. {emoji} **{name}**{team_info}\n`{bar}` {value_str}")
    
    return "\n\n".join(lines)


async def _process_faction_contributors_data(
    faction_data: Dict[str, Any],
    stat_name: str,
    participant_player_ids: set,
    stat_values: Dict[str, Dict[int, float]]
) -> None:
    """Helper function to process contributors data from faction API response.
    
    Args:
        faction_data: Faction API response dict
        stat_name: Name of the stat being processed
        participant_player_ids: Set of participant player IDs to filter for
        stat_values: Dict to update with stat_name -> {player_id: value}
    """
    # Process contributors dict
    # Contributors structure: {"stat_name": {"player_id": {"contributed": value, "in_faction": 1}}}
    contributors = faction_data.get("contributors", {})
    if isinstance(contributors, dict):
        # Get the contributors for this specific stat
        stat_contributors = contributors.get(stat_name, {})
        if isinstance(stat_contributors, dict):
            for player_id_str, contributor_data in stat_contributors.items():
                try:
                    player_id = int(player_id_str)
                    if player_id in participant_player_ids:
                        # contributor_data is {"contributed": value, "in_faction": 1}
                        if isinstance(contributor_data, dict):
                            value = contributor_data.get("contributed")
                            if isinstance(value, (int, float)):
                                if stat_name not in stat_values:
                                    stat_values[stat_name] = {}
                                stat_values[stat_name][player_id] = float(value)
                except (ValueError, KeyError, TypeError) as e:
                    print(f"Warning: Could not process contributor {player_id_str} for stat {stat_name}: {e}")
                    continue


async def _collect_contributor_stats_for_competition(
    bot: commands.Bot,
    comp: Dict[str, Any],
    participants: List[Dict[str, Any]],
    all_keys: List[tuple],
    key_manager: TornKeyManager,
    client: TornAPIClient,
    key_owner_factions: Dict[str, int]
) -> Dict[str, Any]:
    """Collect contributor stats for a competition using faction API calls.
    
    Args:
        bot: Bot instance
        comp: Competition dict
        participants: List of participant dicts
        all_keys: List of (key_alias, key_value) tuples
        key_manager: TornKeyManager instance
        client: TornAPIClient instance
        key_owner_factions: Map of key_alias -> faction_id
    
    Returns:
        Dict with update results
    """
    now_utc = int(datetime.utcnow().timestamp())
    tracked_stat = comp["tracked_stat"]
    
    # Group participants by faction_id
    participants_by_faction = {}
    participants_without_faction = []
    
    for participant in participants:
        player_id = participant["player_id"]
        async with bot.database.connection.execute(
            "SELECT faction_id FROM players WHERE player_id = ?", (player_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                faction_id = row[0]
                if faction_id not in participants_by_faction:
                    participants_by_faction[faction_id] = []
                participants_by_faction[faction_id].append(participant)
            else:
                participants_without_faction.append({
                    "player_id": player_id,
                    "reason": "Player not in any faction or faction_id not set in database"
                })
    
    participants_updated = 0
    factions_processed = 0
    factions_failed = []
    participants_failed = participants_without_faction.copy()
    
    # Determine which stats to fetch
    if tracked_stat == "gym_e_spent":
        stats_to_fetch = ["gymstrength", "gymspeed", "gymdefense", "gymdexterity"]
    else:
        stats_to_fetch = [tracked_stat]
    
    # Process each faction
    for faction_id, faction_participants in participants_by_faction.items():
        participant_player_ids = {p["player_id"] for p in faction_participants}
        
        # Find best key for this faction (prefer key from same faction)
        selected_key = find_best_key_for_faction(all_keys, key_manager, faction_id, key_owner_factions)
        
        if not selected_key:
            factions_failed.append({
                "faction_id": faction_id,
                "reason": "No API key with faction permission available",
                "participant_count": len(faction_participants)
            })
            for p in faction_participants:
                participants_failed.append({
                    "player_id": p["player_id"],
                    "reason": "No API key with faction permission available for this faction"
                })
            continue
        
        # Try to fetch stats for this faction
        faction_success = False
        last_error = None
        keys_tried = []
        
        for attempt_key_alias, attempt_key_value in all_keys:
            if not key_manager.has_permission(attempt_key_alias, "faction"):
                continue
            
            if attempt_key_alias in keys_tried:
                continue
            
            keys_tried.append(attempt_key_alias)
            
            try:
                # Collect stat values for all required stats
                stat_values = {}  # stat_name -> {player_id: value}
                
                for stat_idx, stat_name in enumerate(stats_to_fetch):
                    # First call includes basic selection to get members
                    # Subsequent calls only need contributors
                    selections = ["basic", "contributors"] if stat_idx == 0 else ["contributors"]
                    
                    try:
                        faction_data = await client.get_faction(
                            attempt_key_value,
                            faction_id,
                            selections=selections,
                            stat=stat_name
                        )
                        
                        # Process members dict (only on first call)
                        if stat_idx == 0 and "members" in faction_data:
                            members = faction_data.get("members", {})
                            await process_faction_members(bot, members, faction_id)
                        
                        # Process contributors dict
                        await _process_faction_contributors_data(
                            faction_data, stat_name, participant_player_ids, stat_values
                        )
                        
                        # Rate limit delay
                        await asyncio.sleep(1.1)
                        
                    except TornAPIError as api_error:
                        error_code = getattr(api_error, 'error_code', None)
                        error_msg = str(api_error)
                        
                        # Error 5 = Rate limit - wait and retry
                        if error_code == 5 or "rate limit" in error_msg.lower() or "too many requests" in error_msg.lower():
                            print(f"Rate limit hit for faction {faction_id}, waiting 60 seconds...")
                            await asyncio.sleep(60)
                            # Retry with same key
                            try:
                                retry_faction_data = await client.get_faction(
                                    attempt_key_value,
                                    faction_id,
                                    selections=selections,
                                    stat=stat_name
                                )
                                # Process members dict if this is the first call
                                if stat_idx == 0 and "members" in retry_faction_data:
                                    members = retry_faction_data.get("members", {})
                                    await process_faction_members(bot, members, faction_id)
                                # Process contributors dict
                                await _process_faction_contributors_data(
                                    retry_faction_data, stat_name, participant_player_ids, stat_values
                                )
                            except Exception as retry_error:
                                # Still failed, try next key
                                last_error = f"Rate limit retry failed: {str(retry_error)}"
                                continue
                        
                        # Error 7 or 16 = Permission issue - try next key
                        if error_code in [7, 16] or "permission" in error_msg.lower() or "access level" in error_msg.lower():
                            last_error = f"Permission denied (Error {error_code}): {error_msg}"
                            continue  # Try next key
                        
                        # Other API error - try next key
                        last_error = f"API Error {error_code}: {error_msg}"
                        continue
                
                # Update database for each participant
                masked_key = key_manager.mask_key(attempt_key_value)
                timestamp = now_utc
                
                for participant in faction_participants:
                    player_id = participant["player_id"]
                    
                    if tracked_stat == "gym_e_spent":
                        # Calculate sum for this player
                        total_e = 0.0
                        all_stats_present = True
                        
                        for gym_stat in stats_to_fetch:
                            if gym_stat in stat_values and player_id in stat_values[gym_stat]:
                                value = stat_values[gym_stat][player_id]
                                total_e += value
                                # Store individual gym stat in history
                                await bot.database.append_player_contributor_history(
                                    player_id=player_id,
                                    stat_name=gym_stat,
                                    value=value,
                                    faction_id=faction_id,
                                    data_source=masked_key,
                                    timestamp=timestamp
                                )
                            else:
                                all_stats_present = False
                                break
                        
                        if all_stats_present:
                            final_value = total_e
                            # Store sum in history
                            await bot.database.append_player_contributor_history(
                                player_id=player_id,
                                stat_name="gym_e_spent",
                                value=final_value,
                                faction_id=faction_id,
                                data_source=masked_key,
                                timestamp=timestamp
                            )
                        else:
                            final_value = None
                    else:
                        # Single stat
                        if tracked_stat in stat_values and player_id in stat_values[tracked_stat]:
                            final_value = stat_values[tracked_stat][player_id]
                            # Store in history
                            await bot.database.append_player_contributor_history(
                                player_id=player_id,
                                stat_name=tracked_stat,
                                value=final_value,
                                faction_id=faction_id,
                                data_source=masked_key,
                                timestamp=timestamp
                            )
                        else:
                            final_value = None
                    
                    # Update start stat if not set and competition has started
                    if final_value is not None:
                        start_stat = await bot.database.get_competition_start_stat(comp["id"], player_id)
                        if start_stat is None and now_utc >= comp["start_date"]:
                            await bot.database.set_competition_start_stat(
                                comp["id"], player_id, final_value, stat_source="contributors"
                            )
                        
                        participants_updated += 1
                    else:
                        participants_failed.append({
                            "player_id": player_id,
                            "reason": f"No contributor value found for stat '{tracked_stat}' in faction {faction_id}"
                        })
                
                faction_success = True
                factions_processed += 1
                break  # Success, move to next faction
                
            except Exception as e:
                last_error = f"Unexpected error: {str(e)}"
                import traceback
                print(f"Error processing faction {faction_id}: {e}")
                traceback.print_exc()
                continue  # Try next key
        
        # If all keys failed for this faction
        if not faction_success:
            factions_failed.append({
                "faction_id": faction_id,
                "reason": last_error or "All API keys failed",
                "participant_count": len(faction_participants),
                "keys_tried": len(keys_tried)
            })
            for p in faction_participants:
                participants_failed.append({
                    "player_id": p["player_id"],
                    "reason": f"Faction {faction_id} failed: {last_error or 'All API keys failed'}"
                })
    
    return {
        "participants_updated": participants_updated,
        "factions_processed": factions_processed,
        "factions_failed": factions_failed,
        "participants_failed": participants_failed
    }


async def _update_competition_stats_helper(bot: commands.Bot) -> Dict[str, Any]:
    """Helper function to update stats for all active competitions using faction contributors endpoint.
    
    Returns:
        Dict with update results including failures
    """
    if not hasattr(bot, 'database') or not bot.database:
        return {"error": "Database not available", "competitions_updated": 0, "participants_updated": 0, "participants_failed": 0}
    
    try:
        # Get all active competitions
        active_competitions = await bot.database.list_competitions(status="active")
        
        if not active_competitions:
            return {
                "message": "No active competitions found",
                "competitions_updated": 0,
                "participants_updated": 0,
                "participants_failed": 0
            }
        
        # Initialize key manager
        key_manager = TornKeyManager()
        client = TornAPIClient()
        
        # Get all available keys with faction permission
        all_keys, key_owner_factions = await get_all_faction_keys(key_manager, client)
        
        if not all_keys:
            await client.close()
            return {
                "message": "No API keys with faction permission found",
                "competitions_updated": 0,
                "participants_updated": 0,
                "participants_failed": 0
            }
        
        competitions_updated = 0
        total_participants_updated = 0
        total_participants_failed = []
        total_factions_processed = 0
        total_factions_failed = []
        
        try:
            for comp in active_competitions:
                now_utc = int(datetime.utcnow().timestamp())
                
                # Skip if competition hasn't started yet
                if now_utc < comp["start_date"]:
                    continue
                
                # Get all participants for this competition
                participants = await bot.database.get_competition_participants(comp["id"])
                
                if not participants:
                    continue
                
                # Collect contributor stats for this competition
                result = await _collect_contributor_stats_for_competition(
                    bot, comp, participants, all_keys, key_manager, client, key_owner_factions
                )
                
                total_participants_updated += result["participants_updated"]
                total_factions_processed += result["factions_processed"]
                total_factions_failed.extend(result["factions_failed"])
                total_participants_failed.extend(result["participants_failed"])
                
                competitions_updated += 1
                
        finally:
            await client.close()
        
        # Build result message
        message_parts = [f"Updated stats for {competitions_updated} competition(s) and {total_participants_updated} participant(s)"]
        message_parts.append(f"Processed {total_factions_processed} faction(s)")
        
        if total_factions_failed:
            message_parts.append(f"\n⚠️ Failed to process {len(total_factions_failed)} faction(s)")
        
        if total_participants_failed:
            message_parts.append(f"\n⚠️ Failed to update {len(total_participants_failed)} participant(s)")
        
        # Check for user intervention needed
        user_intervention_needed = []
        for faction_fail in total_factions_failed:
            if "No API key" in faction_fail.get("reason", "") or "All API keys failed" in faction_fail.get("reason", ""):
                user_intervention_needed.append(faction_fail)
        
        return {
            "message": " ".join(message_parts),
            "competitions_updated": competitions_updated,
            "participants_updated": total_participants_updated,
            "factions_processed": total_factions_processed,
            "factions_failed": total_factions_failed,
            "participants_failed": total_participants_failed,
            "participants_failed_count": len(total_participants_failed),
            "user_intervention_needed": user_intervention_needed
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "error": str(e),
            "message": f"Error during update: {str(e)}",
            "competitions_updated": competitions_updated if 'competitions_updated' in locals() else 0,
            "participants_updated": total_participants_updated if 'total_participants_updated' in locals() else 0,
            "participants_failed": total_participants_failed if 'total_participants_failed' in locals() else [],
            "participants_failed_count": len(total_participants_failed) if 'total_participants_failed' in locals() else 0
        }


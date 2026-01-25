"""Organized crime tracking commands for Discord bot."""

import discord
from discord.ext import commands
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
import json
import asyncio
from torn_api import TornAPIClient, TornKeyManager
from torn_api.client import TornAPIError
from commands.competition_utils import require_admin


async def sync_organized_crimes_helper(bot: commands.Bot) -> Dict[str, Any]:
    """Helper function to sync organized crime data for all tracked factions.
    
    Only uses API keys with faction permission from TornKeyManager.
    
    Returns:
        Dict with sync results including successes and failures
    """
    if not hasattr(bot, 'database') or not bot.database:
        return {"error": "Database not available", "factions_synced": 0, "factions_failed": 0}
    
    try:
        # Get all tracked factions
        tracked_factions = await bot.database.get_all_tracked_factions()
        
        if not tracked_factions:
            return {
                "message": "No factions with organized crime tracking enabled",
                "factions_synced": 0,
                "factions_failed": 0
            }
        
        # Initialize API client and key manager
        key_manager = TornKeyManager()
        client = TornAPIClient()
        
        # Get all keys with faction permission
        full_access_keys = []
        
        for key_alias, key_meta in key_manager.metadata.get("keys", {}).items():
            if key_manager.has_permission(key_alias, "faction"):
                key_value = key_manager.get_key_value(key_alias)
                if key_value:
                    full_access_keys.append((key_alias, key_value))
        
        if not full_access_keys:
            await client.close()
            return {
                "message": "No API keys with faction permission found. Faction permission is required for organized crime tracking.",
                "factions_synced": 0,
                "factions_failed": len(tracked_factions)
            }
        
        print(f"Found {len(full_access_keys)} key(s) with faction permission for organized crime sync")
        for key_alias, _ in full_access_keys:
            print(f"  - Key '{key_alias}'")
        
        factions_synced = 0
        factions_failed = []
        crimes_updated = 0
        events_recorded = 0
        
        try:
            # Group keys by faction to avoid duplicate syncing
            faction_keys_map = {}  # faction_id -> (key_alias, key_value)
            
            # First, get faction ID for each key by calling faction/basic (v2)
            for key_alias, key_value in full_access_keys:
                try:
                    print(f"Getting faction ID for key '{key_alias}'")
                    faction_basic = await client.get_faction(key_value, None, selections=["basic"], use_v2=True)
                    if isinstance(faction_basic, dict) and "basic" in faction_basic:
                        basic_data = faction_basic["basic"]
                        if isinstance(basic_data, dict) and "id" in basic_data:
                            key_faction_id = basic_data["id"]
                            if key_faction_id not in faction_keys_map:
                                faction_keys_map[key_faction_id] = (key_alias, key_value)
                                print(f"Key '{key_alias}' belongs to faction {key_faction_id}")
                            await asyncio.sleep(0.5)  # Rate limit delay
                except Exception as e:
                    print(f"Error getting faction ID for key '{key_alias}': {e}")
                    continue
            
            # Sync each tracked faction using appropriate key
            for config in tracked_factions:
                faction_id = config['faction_id']
                guild_id = config['guild_id']
                
                # Store timestamp BEFORE starting sync (will be used to update last_sync at end)
                sync_start_timestamp = int(datetime.utcnow().timestamp())
                
                success = False
                last_error = None
                
                # Find a key for this faction
                if faction_id not in faction_keys_map:
                    last_error = f"No API key found for faction {faction_id}"
                    print(f"Error: {last_error}")
                    factions_failed.append({
                        "faction_id": faction_id,
                        "reason": last_error
                    })
                    continue
                
                key_alias, key_value = faction_keys_map[faction_id]
                masked_key = key_manager.mask_key(key_value)
                
                # Get last_sync timestamp to use as 'from' parameter
                last_sync = config.get('last_sync')
                if last_sync:
                    print(f"Syncing faction {faction_id} with key '{key_alias}' (from timestamp: {last_sync})")
                else:
                    print(f"Syncing faction {faction_id} with key '{key_alias}' (full sync, no last_sync)")
                
                try:
                    # Fetch all pages of faction crime data with pagination
                    all_crimes = []
                    offset = 0
                    has_more = True
                    
                    while has_more:
                        print(f"Calling API: /v2/faction/crimes (offset={offset}, from={last_sync})")
                        crime_data = await client.get_faction_crimes(
                            key_value, 
                            offset=offset, 
                            sort="DESC",
                            from_timestamp=last_sync
                        )
                        print(f"API call successful. Response type: {type(crime_data)}")
                        
                        # Check if response is valid
                        if not isinstance(crime_data, dict):
                            last_error = f"Invalid API response format: expected dict, got {type(crime_data)}"
                            print(f"Error: {last_error}")
                            break
                        
                        # Check for error in response
                        if "error" in crime_data:
                            error_info = crime_data.get("error", {})
                            error_code = error_info.get("code") if isinstance(error_info, dict) else None
                            error_text = error_info.get("error") if isinstance(error_info, dict) else str(error_info)
                            last_error = f"API returned error in response: Code {error_code}, {error_text}"
                            print(f"Error in API response: {last_error}")
                            break
                        
                        # Extract crimes from this page
                        page_crimes = crime_data.get("crimes", [])
                        if isinstance(page_crimes, list):
                            all_crimes.extend(page_crimes)
                            print(f"Found {len(page_crimes)} crime(s) on this page (total so far: {len(all_crimes)})")
                        
                        # Check for next page
                        metadata = crime_data.get("_metadata", {})
                        links = metadata.get("links", {}) if isinstance(metadata, dict) else {}
                        next_link = links.get("next") if isinstance(links, dict) else None
                        
                        if next_link:
                            # Extract offset from next link or increment
                            # URL format: ...&offset=100&sort=desc
                            if "offset=" in next_link:
                                try:
                                    offset_part = next_link.split("offset=")[1].split("&")[0]
                                    offset = int(offset_part)
                                except (ValueError, IndexError):
                                    offset += 100  # Default increment
                            else:
                                offset += 100  # Default increment
                            await asyncio.sleep(1.1)  # Rate limit delay
                        else:
                            has_more = False
                    
                    if last_error:
                        continue
                    
                    # Process all collected crimes
                    crime_data_combined = {"crimes": all_crimes}
                    result = await _process_faction_crime_data(
                        bot, faction_id, crime_data_combined, masked_key
                    )
                    
                    crimes_updated += result.get("crimes_updated", 0)
                    events_recorded += result.get("events_recorded", 0)
                    
                    print(f"Successfully processed faction {faction_id}: {result.get('crimes_updated', 0)} crimes updated, {result.get('events_recorded', 0)} events recorded")
                    
                    # Check for missing items and send reminders
                    await _check_and_notify_missing_items(
                        bot, client, faction_id, guild_id, all_crimes, key_value
                    )
                    
                    # Update sync timestamp at END with timestamp from BEFORE sync started
                    await bot.database.update_organized_crime_config_sync_time(
                        faction_id, guild_id, sync_start_timestamp
                    )
                    
                    factions_synced += 1
                    success = True
                    
                    # Rate limit delay
                    await asyncio.sleep(1.1)
                    break  # Success, move to next faction
                    
                except TornAPIError as api_error:
                    error_code = getattr(api_error, 'error_code', None)
                    error_msg = str(api_error)
                    
                    # Try to extract error code from message if not set
                    if error_code is None and "Error" in error_msg:
                        try:
                            # Extract from "Torn API Error 7: ..."
                            parts = error_msg.split("Error")
                            if len(parts) > 1:
                                code_part = parts[1].split(":")[0].strip()
                                error_code = int(code_part)
                        except (ValueError, IndexError):
                            pass
                    
                    print(f"TornAPIError for faction {faction_id} with key '{key_alias}': Code {error_code}, Message: {error_msg}")
                    
                    # Error 7 = Permission denied - try next key
                    if error_code == 7:
                        last_error = f"API Error 7: Permission denied with key '{key_alias}'"
                        print(f"Error 7: {last_error}. Trying next key...")
                        continue  # Try next key
                    
                    # Error 5 = Rate limit - wait and retry with same key
                    if error_code == 5 or "rate limit" in error_msg.lower():
                        print(f"Rate limit hit for faction {faction_id}, waiting 60 seconds...")
                        await asyncio.sleep(60)
                        # Retry once with same key (simplified - will be handled in main loop)
                        last_error = f"Rate limit hit, will retry on next sync"
                        print(f"Rate limit: {last_error}")
                        continue  # Will retry on next sync cycle
                    else:
                        # Other API error - try next key
                        last_error = f"API Error {error_code}: {error_msg}"
                        print(f"API Error: {last_error}")
                        continue  # Try next key
                
                except Exception as e:
                    last_error = f"Unexpected error with key '{key_alias}': {str(e)}"
                    import traceback
                    print(f"Unexpected error syncing faction {faction_id} with key {key_alias}: {e}")
                    traceback.print_exc()
                    continue  # Try next key
                
                # If sync failed
                if not success:
                    if last_error:
                        reason = last_error
                    else:
                        reason = f"Sync failed with {len(full_access_keys)} available key(s)"
                    
                    factions_failed.append({
                        "faction_id": faction_id,
                        "reason": reason
                    })
        
        finally:
            await client.close()
        
        # Check for frequent leavers and send notifications
        await _check_and_notify_frequent_leavers(bot, tracked_factions)
        
        message_parts = [
            f"Synced {factions_synced} faction(s)",
            f"Updated {crimes_updated} crime(s)",
            f"Recorded {events_recorded} event(s)"
        ]
        
        if factions_failed:
            message_parts.append(f"\n⚠️ Failed to sync {len(factions_failed)} faction(s)")
        
        return {
            "message": " ".join(message_parts),
            "factions_synced": factions_synced,
            "factions_failed": len(factions_failed),
            "factions_failed_details": factions_failed,
            "crimes_updated": crimes_updated,
            "events_recorded": events_recorded
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "error": str(e),
            "message": f"Error during sync: {str(e)}",
            "factions_synced": factions_synced if 'factions_synced' in locals() else 0,
            "factions_failed": len(factions_failed) if 'factions_failed' in locals() else 0
        }


async def _process_faction_crime_data(
    bot: commands.Bot,
    faction_id: int,
    crime_data: Dict[str, Any],
    data_source: str
) -> Dict[str, Any]:
    """Process faction crime data from API and update database.
    
    Returns:
        Dict with counts of crimes_updated and events_recorded
    """
    crimes_updated = 0
    events_recorded = 0
    
    # Get current crimes from database
    current_crimes_db = await bot.database.get_organized_crimes_current(faction_id)
    current_crimes_map = {c['crime_id']: c for c in current_crimes_db}
    
    # Process crimes from API
    # API structure: {"crimes": [crime_objects]}
    api_crimes = crime_data.get("crimes", [])
    
    if not isinstance(api_crimes, list):
        print(f"ERROR: api_crimes is not a list, got {type(api_crimes).__name__}")
        return {"crimes_updated": 0, "events_recorded": 0}
    
    print(f"Processing {len(api_crimes)} crime(s) for faction {faction_id}")
    now_timestamp = int(datetime.utcnow().timestamp())
    
    # Map API status values to database status values
    status_map = {
        "available": "planning",
        "recruiting": "planning",
        "planning": "planning",
        "ready": "ready",  # Assuming ready_at means ready status
        "completed": "completed",
        "successful": "completed",
        "failure": "failed",
        "expired": "cancelled"
    }
    
    processed_count = 0
    skipped_count = 0
    new_crimes_count = 0
    existing_crimes_count = 0
    
    for crime_info in api_crimes:
        if not isinstance(crime_info, dict):
            skipped_count += 1
            continue
        
        try:
            # Extract crime ID (API uses "id" not "crime_id")
            crime_id = crime_info.get("id")
            if crime_id is None:
                skipped_count += 1
                print(f"  Skipping crime: missing 'id' field")
                continue
            crime_id = int(crime_id)
        except (ValueError, TypeError) as e:
            skipped_count += 1
            print(f"  Skipping crime: invalid id '{crime_info.get('id')}': {e}")
            continue
        
        processed_count += 1
        if processed_count <= 3:  # Log first 3 crimes for debugging
            print(f"  Processing crime {crime_id}: name='{crime_info.get('name', 'N/A')}', status='{crime_info.get('status', 'N/A')}'")
        
        # Extract crime data
        crime_name = crime_info.get("name", f"Crime {crime_id}")
        crime_type = None  # API doesn't provide crime_type in new structure
        
        # Extract participants from slots
        slots = crime_info.get("slots", [])
        if not isinstance(slots, list):
            slots = []
        
        participants = []
        for slot in slots:
            if isinstance(slot, dict):
                user_id = slot.get("user")
                # user should be null/None if slot is not filled, or an integer (player ID) if filled
                # Handle edge case where API might return a dict instead of null
                if user_id is not None:
                    # If it's already an integer, use it
                    if isinstance(user_id, int):
                        participants.append(user_id)
                    # If it's a dict (unexpected API behavior), try to extract ID
                    elif isinstance(user_id, dict):
                        # Try common field names that might contain the player ID
                        extracted_id = user_id.get("id") or user_id.get("user_id") or user_id.get("player_id")
                        if extracted_id is not None:
                            try:
                                participants.append(int(extracted_id))
                            except (ValueError, TypeError):
                                pass
                    else:
                        # Try to convert to int if it's a string or other type
                        try:
                            participants.append(int(user_id))
                        except (ValueError, TypeError):
                            pass
        
        participant_count = len(participants)
        required_participants = len(slots)  # Number of slots = required participants
        
        # Extract timestamps
        time_started = crime_info.get("created_at")
        time_completed = crime_info.get("executed_at")
        ready_at = crime_info.get("ready_at")
        
        # Map status (always lowercase API status first)
        api_status = crime_info.get("status", "planning")
        if isinstance(api_status, str):
            api_status = api_status.lower()
        else:
            api_status = "planning"
        
        # If ready_at is set, status should be "ready"
        if ready_at and api_status in ["planning", "recruiting"]:
            status = "ready"
        else:
            status = status_map.get(api_status, "planning")
        
        # Extract rewards (API uses "rewards" plural, can be null)
        rewards = crime_info.get("rewards")
        reward_money = None
        reward_respect = None
        reward_other = None
        
        if isinstance(rewards, dict):
            reward_money = rewards.get("money")
            reward_respect = rewards.get("respect")
            # Extract other reward fields
            reward_other_dict = {k: v for k, v in rewards.items() if k not in ["money", "respect"]}
            if reward_other_dict:
                reward_other = json.dumps(reward_other_dict)
        
        # Check if this is a new crime
        current_crime = current_crimes_map.get(crime_id)
        
        if processed_count <= 3:
            print(f"    Crime {crime_id}: current_crime exists={current_crime is not None}, status={status}, participants={len(participants)}")
            if current_crime:
                print(f"      DB status={current_crime.get('status')}, DB participants={len(current_crime.get('participants', []))}, last_updated={current_crime.get('last_updated')}")
        
        if not current_crime:
            # New crime - create it
            new_crimes_count += 1
            if processed_count <= 3 or new_crimes_count <= 5:
                print(f"      INSERTING new crime {crime_id}: name='{crime_name}', status={status}, participants={len(participants)}")
            
            # Validate required fields before insertion
            if not crime_name:
                print(f"      WARNING: Crime {crime_id} has no name, using default")
                crime_name = f"Crime {crime_id}"
            
            if not status:
                print(f"      WARNING: Crime {crime_id} has no status, using 'planning'")
                status = "planning"
            
            try:
                await bot.database.upsert_organized_crime_current(
                    faction_id=faction_id,
                    crime_id=crime_id,
                    crime_name=crime_name,
                    crime_type=crime_type,
                    participants=participants,
                    participant_count=participant_count,
                    required_participants=required_participants,
                    time_started=time_started,
                    time_completed=time_completed,
                    status=status,
                    reward_money=reward_money,
                    reward_respect=reward_respect,
                    reward_other=reward_other,
                    data_source=data_source
                )
                
                # Record creation event
                await bot.database.append_organized_crime_history(
                    faction_id=faction_id,
                    crime_id=crime_id,
                    event_type="created",
                    new_status=status,
                    new_participants=participants,
                    reward_money=reward_money,
                    reward_respect=reward_respect,
                    reward_other=reward_other,
                    data_source=data_source,
                    event_timestamp=time_started or now_timestamp
                )
                events_recorded += 1
                crimes_updated += 1
                
                if processed_count <= 3 or new_crimes_count <= 5:
                    print(f"      ✓ Successfully inserted crime {crime_id} and created history event")
            except Exception as e:
                print(f"      ✗ ERROR inserting crime {crime_id}: {e}")
                import traceback
                traceback.print_exc()
                skipped_count += 1
        
        else:
            # Existing crime - check for changes
            existing_crimes_count += 1
            changes_detected = False
            
            # Check status change
            db_status = current_crime.get('status', '')
            if processed_count <= 3:
                print(f"      Comparing status: DB='{db_status}' vs API='{status}' (from '{api_status}')")
            if db_status != status:
                await bot.database.append_organized_crime_history(
                    faction_id=faction_id,
                    crime_id=crime_id,
                    event_type="status_changed",
                    old_status=current_crime['status'],
                    new_status=status,
                    data_source=data_source,
                    event_timestamp=now_timestamp
                )
                events_recorded += 1
                changes_detected = True
            
            # Check participant changes
            old_participants = current_crime.get('participants', [])
            if not isinstance(old_participants, list):
                # Handle case where participants might be stored as JSON string
                try:
                    if isinstance(old_participants, str):
                        old_participants = json.loads(old_participants)
                    else:
                        old_participants = []
                except (json.JSONDecodeError, TypeError):
                    old_participants = []
            
            new_participants = participants
            
            if processed_count <= 3:
                print(f"      Comparing participants: DB={set(old_participants)} vs API={set(new_participants)}")
            
            if set(old_participants) != set(new_participants):
                # Find joins and leaves
                joined = [p for p in new_participants if p not in old_participants]
                left = [p for p in old_participants if p not in new_participants]
                
                # Record joins
                for player_id in joined:
                    await bot.database.append_organized_crime_history(
                        faction_id=faction_id,
                        crime_id=crime_id,
                        event_type="participant_joined",
                        player_id=player_id,
                        old_participants=old_participants,
                        new_participants=new_participants,
                        data_source=data_source,
                        event_timestamp=now_timestamp
                    )
                    events_recorded += 1
                
                # Record leaves
                for player_id in left:
                    await bot.database.append_organized_crime_history(
                        faction_id=faction_id,
                        crime_id=crime_id,
                        event_type="participant_left",
                        player_id=player_id,
                        old_participants=old_participants,
                        new_participants=new_participants,
                        data_source=data_source,
                        event_timestamp=now_timestamp
                    )
                    events_recorded += 1
                    changes_detected = True
            
            # Check completion (status changed to completed/failed/cancelled)
            final_statuses = ["completed", "failed", "cancelled"]
            if status in final_statuses and current_crime['status'] not in final_statuses:
                # Map event type based on status
                if status == "completed":
                    event_type = "completed"
                elif status == "failed":
                    event_type = "failed"
                else:  # cancelled
                    event_type = "cancelled"
                
                await bot.database.append_organized_crime_history(
                    faction_id=faction_id,
                    crime_id=crime_id,
                    event_type=event_type,
                    old_status=current_crime['status'],
                    new_status=status,
                    reward_money=reward_money,
                    reward_respect=reward_respect,
                    reward_other=reward_other,
                    data_source=data_source,
                    event_timestamp=time_completed or now_timestamp
                )
                events_recorded += 1
                changes_detected = True
                
                # Update participant stats
                for player_id in participants:
                    await bot.database.update_participant_crime_stats(
                        faction_id=faction_id,
                        player_id=player_id,
                        crime_type=crime_type,
                        crimes_completed=1 if status == "completed" else 0,
                        crimes_failed=1 if status == "failed" else 0,
                        total_reward_money=reward_money or 0,
                        total_reward_respect=reward_respect or 0
                    )
                
                # Remove from current table
                await bot.database.delete_organized_crime_current(faction_id, crime_id)
            
            # Update current crime if there were changes or if it's been more than 5 minutes
            last_updated = current_crime.get('last_updated', 0)
            needs_update = changes_detected or (last_updated < (now_timestamp - 300))
            
            if processed_count <= 3:
                print(f"      Update check: changes_detected={changes_detected}, last_updated={last_updated}, now={now_timestamp}, needs_update={needs_update}")
            
            if needs_update:
                try:
                    await bot.database.upsert_organized_crime_current(
                        faction_id=faction_id,
                        crime_id=crime_id,
                        crime_name=crime_name,
                        crime_type=crime_type,
                        participants=participants,
                        participant_count=participant_count,
                        required_participants=required_participants,
                        time_started=time_started,
                        time_completed=time_completed,
                        status=status,
                        reward_money=reward_money,
                        reward_respect=reward_respect,
                        reward_other=reward_other,
                        data_source=data_source
                    )
                    # Count as updated if there were changes OR if it's been more than 5 minutes since last update
                    if changes_detected or (last_updated < (now_timestamp - 300)):
                        crimes_updated += 1
                    if processed_count <= 3:
                        print(f"      Successfully updated crime {crime_id} in database")
                except Exception as e:
                    print(f"      ERROR updating crime {crime_id}: {e}")
                    import traceback
                    traceback.print_exc()
    
    print(f"Processing complete: processed={processed_count}, skipped={skipped_count}, new={new_crimes_count}, existing={existing_crimes_count}, updated={crimes_updated}, events={events_recorded}")
    return {
        "crimes_updated": crimes_updated,
        "events_recorded": events_recorded,
        "new_crimes": new_crimes_count,
        "existing_crimes": existing_crimes_count
    }


async def _check_and_notify_missing_items(
    bot: commands.Bot,
    client: TornAPIClient,
    faction_id: int,
    guild_id: str,
    crimes: List[Dict[str, Any]],
    api_key: str
):
    """Check for missing items in crime slots and send Discord reminders."""
    # Get config to check if missing item reminders are enabled
    config = await bot.database.get_organized_crime_config(faction_id, guild_id)
    if not config:
        return
    
    channel_id = config.get('missing_item_reminder_channel_id')
    if not channel_id:
        return  # No channel configured for missing item reminders
    
    # Get guild and channel
    guild = bot.get_guild(int(guild_id))
    if not guild:
        return
    
    channel = guild.get_channel(int(channel_id))
    if not channel:
        return
    
    # Process crimes with status "recruiting" or "planning"
    for crime_info in crimes:
        if not isinstance(crime_info, dict):
            continue
        
        crime_status = crime_info.get("status", "").lower()
        if crime_status not in ["recruiting", "planning"]:
            continue
        
        crime_id = crime_info.get("id")
        crime_name = crime_info.get("name", f"Crime {crime_id}")
        slots = crime_info.get("slots", [])
        
        if not isinstance(slots, list):
            continue
        
        # Check each slot for missing items
        for slot in slots:
            if not isinstance(slot, dict):
                continue
            
            item_requirement = slot.get("item_requirement")
            user_id = slot.get("user")
            position = slot.get("position", "Unknown")
            
            # Skip if no item requirement, no user assigned, or item is available
            if not item_requirement or not isinstance(item_requirement, dict):
                continue
            
            if user_id is None:
                continue
            
            if item_requirement.get("is_available", True):
                continue  # Item is available, no reminder needed
            
            # Item is missing - get item name and user discord_id
            item_id = item_requirement.get("id")
            if not item_id:
                continue
            
            try:
                # Get item name from cache or API
                item = await bot.database.get_item(item_id)
                if not item:
                    # Not in cache, fetch from API
                    try:
                        item_data = await client.get_item(api_key, item_id)
                        # API returns items as a dict with item_id as key
                        if isinstance(item_data, dict) and str(item_id) in item_data:
                            item_info = item_data[str(item_id)]
                            item_name = item_info.get("name", f"Item {item_id}")
                            item_description = item_info.get("description")
                            item_type = item_info.get("type")
                            market_value = item_info.get("market_value")
                            
                            # Cache the item
                            await bot.database.upsert_item(
                                item_id, item_name, item_description, item_type, market_value
                            )
                            item = {"name": item_name}
                        else:
                            item = {"name": f"Item {item_id}"}
                    except Exception as e:
                        print(f"Error fetching item {item_id} from API: {e}")
                        item = {"name": f"Item {item_id}"}
                
                item_name = item.get("name", f"Item {item_id}")
                
                # Get user discord_id from cache or API
                discord_id = await bot.database.get_player_discord_id(user_id)
                if not discord_id:
                    # Not in cache, fetch from API
                    try:
                        user_data = await client.get_user_discord(api_key, user_id)
                        if isinstance(user_data, dict) and "discord" in user_data:
                            discord_obj = user_data["discord"]
                            if isinstance(discord_obj, dict) and "discord_id" in discord_obj:
                                discord_id = str(discord_obj["discord_id"])
                                # Cache the discord_id
                                await bot.database.update_player_discord_id(user_id, discord_id)
                    except Exception as e:
                        print(f"Error fetching discord info for user {user_id}: {e}")
                
                if not discord_id:
                    continue  # No discord_id found, skip
                
                # Send Discord message
                try:
                    message = f"<@{discord_id}> You need to get **{item_name}** for the **{position}** slot in crime **{crime_name}** (ID: {crime_id})"
                    await channel.send(message)
                    print(f"Sent missing item reminder to user {user_id} (discord {discord_id}) for item {item_name} in crime {crime_id}")
                except discord.Forbidden:
                    print(f"Missing permissions to send message in channel {channel_id}")
                except Exception as e:
                    print(f"Error sending missing item reminder: {e}")
                
                # Rate limit delay
                await asyncio.sleep(0.5)
                
            except Exception as e:
                print(f"Error processing missing item for slot in crime {crime_id}: {e}")
                import traceback
                traceback.print_exc()


async def _check_and_notify_frequent_leavers(
    bot: commands.Bot,
    tracked_factions: List[Dict[str, Any]]
):
    """Check for frequent leavers and send notifications."""
    for config in tracked_factions:
        faction_id = config['faction_id']
        guild_id = config['guild_id']
        threshold = config['frequent_leaver_threshold']
        window_days = config['tracking_window_days']
        channel_id = config.get('notification_channel_id')
        lead_ids = config.get('faction_lead_discord_ids', [])
        
        if not channel_id or not lead_ids:
            continue  # Skip if no notification channel or leads configured
        
        try:
            # Get frequent leavers
            leavers = await bot.database.get_frequent_leavers(faction_id, threshold, window_days)
            
            if not leavers:
                continue
            
            # Get guild and channel
            guild = bot.get_guild(int(guild_id))
            if not guild:
                continue
            
            channel = guild.get_channel(int(channel_id))
            if not channel:
                continue
            
            # Send notification for each leaver
            for leaver in leavers:
                player_id = leaver['player_id']
                leave_count = leaver['leave_count']
                
                # Get recent leaves for this player
                recent_leaves = await bot.database.get_participant_crime_leaves(
                    faction_id, player_id, window_days
                )
                
                # Build embed
                embed = discord.Embed(
                    title="⚠️ Frequent Crime Leaver Detected",
                    color=discord.Color.orange(),
                    description=f"Player has left {leave_count} crime(s) in the last {window_days} days"
                )
                
                embed.add_field(
                    name="Player",
                    value=f"ID: {player_id}",
                    inline=True
                )
                
                embed.add_field(
                    name="Leaves",
                    value=f"{leave_count} (threshold: {threshold})",
                    inline=True
                )
                
                # Add recent crimes left
                if recent_leaves:
                    recent_crimes = []
                    for leave in recent_leaves[:5]:  # Last 5
                        crime_id = leave.get('crime_id', 'Unknown')
                        timestamp = leave.get('event_timestamp', 0)
                        if timestamp:
                            time_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")
                        else:
                            time_str = "Unknown"
                        recent_crimes.append(f"Crime {crime_id} ({time_str})")
                    
                    embed.add_field(
                        name="Recent Crimes Left",
                        value="\n".join(recent_crimes) if recent_crimes else "None",
                        inline=False
                    )
                
                embed.set_footer(text=f"Faction ID: {faction_id}")
                embed.timestamp = datetime.utcnow()
                
                # Mention faction leads
                mentions = " ".join([f"<@{lead_id}>" for lead_id in lead_ids])
                
                try:
                    await channel.send(content=mentions, embed=embed)
                except discord.Forbidden:
                    # Missing permissions, skip
                    pass
                except Exception as e:
                    print(f"Error sending notification for faction {faction_id}: {e}")
        
        except Exception as e:
            print(f"Error checking frequent leavers for faction {faction_id}: {e}")
            import traceback
            traceback.print_exc()


def setup(bot: commands.Bot):
    """Setup function to register organized crime commands."""
    
    @bot.tree.command(name="oc-test-api", description="Test faction crime API call with faction-permission key (Admin only)")
    async def oc_test_api(interaction: discord.Interaction):
        """Test the faction crime API call directly. Returns data for the key owner's faction."""
        if not await require_admin(interaction, "oc-test-api", bot):
            return
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            key_manager = TornKeyManager()
            client = TornAPIClient()
            
            # Get first key with faction permission
            full_key = None
            for key_alias, key_meta in key_manager.metadata.get("keys", {}).items():
                if key_manager.has_permission(key_alias, "faction"):
                    key_value = key_manager.get_key_value(key_alias)
                    if key_value:
                        full_key = (key_alias, key_value)
                        break
            
            if not full_key:
                await interaction.followup.send("❌ No API key with faction permission found.", ephemeral=True)
                await client.close()
                return
            
            key_alias, key_value = full_key
            
            # Get faction ID from faction/basic
            try:
                faction_basic = await client.get_faction(key_value, None, selections=["basic"], use_v2=True)
                faction_id = None
                if isinstance(faction_basic, dict) and "basic" in faction_basic:
                    basic_data = faction_basic["basic"]
                    if isinstance(basic_data, dict) and "id" in basic_data:
                        faction_id = basic_data["id"]
            except Exception as e:
                faction_id = None
                print(f"Could not get faction ID: {e}")
            
            embed = discord.Embed(
                title="🔍 API Test Results",
                color=discord.Color.blue()
            )
            embed.add_field(name="Key Alias", value=key_alias, inline=True)
            embed.add_field(name="Faction ID", value=str(faction_id) if faction_id else "Unknown", inline=True)
            
            try:
                # Test the API call
                # Note: API uses key owner's faction automatically
                print(f"Testing API call: /v2/faction/crimes (faction: {faction_id})")
                crime_data = await client.get_faction_crimes(key_value)
                
                embed.color = discord.Color.green()
                embed.add_field(
                    name="✅ API Call Success",
                    value="The API call succeeded!",
                    inline=False
                )
                
                # Show response structure
                if isinstance(crime_data, dict):
                    response_keys = list(crime_data.keys())
                    embed.add_field(
                        name="Response Keys",
                        value=", ".join(response_keys) if response_keys else "None",
                        inline=False
                    )
                    
                    if "crimes" in crime_data:
                        crimes = crime_data["crimes"]
                        if isinstance(crimes, list):
                            embed.add_field(
                                name="Crimes Found",
                                value=f"{len(crimes)} crime(s)",
                                inline=True
                            )
                            
                            # Show first crime as example
                            if crimes:
                                first_crime = crimes[0]
                                if isinstance(first_crime, dict):
                                    crime_id = first_crime.get("id", "N/A")
                                    crime_name = first_crime.get("name", "N/A")
                                    crime_status = first_crime.get("status", "N/A")
                                    embed.add_field(
                                        name="Sample Crime",
                                        value=f"ID: {crime_id}\nName: {crime_name}\nStatus: {crime_status}",
                                        inline=False
                                    )
                        else:
                            embed.add_field(
                                name="Crimes Data Type",
                                value=f"{type(crimes).__name__} (expected list)",
                                inline=True
                            )
                    else:
                        embed.add_field(
                            name="⚠️ No 'crimes' Key",
                            value="Response does not contain 'crimes' key",
                            inline=False
                        )
                        embed.add_field(
                            name="Full Response",
                            value=f"```json\n{json.dumps(crime_data, indent=2)[:1000]}\n```",
                            inline=False
                        )
                else:
                    embed.add_field(
                        name="⚠️ Unexpected Response Type",
                        value=f"Expected dict, got {type(crime_data).__name__}",
                        inline=False
                    )
                
            except TornAPIError as api_error:
                error_code = getattr(api_error, 'error_code', None)
                error_msg = str(api_error)
                
                # Try to extract error code from message
                if error_code is None and "Error" in error_msg:
                    try:
                        parts = error_msg.split("Error")
                        if len(parts) > 1:
                            code_part = parts[1].split(":")[0].strip()
                            error_code = int(code_part)
                    except (ValueError, IndexError):
                        pass
                
                embed.color = discord.Color.red()
                embed.add_field(
                    name="❌ API Call Failed",
                    value=f"Error Code: {error_code}\nMessage: {error_msg}",
                    inline=False
                )
                
                if error_code == 7:
                    embed.add_field(
                        name="💡 Explanation",
                        value=f"Error 7: Permission denied. The API key must be from a member of the faction.",
                        inline=False
                    )
            
            finally:
                await client.close()
            
            await interaction.followup.send(embed=embed, ephemeral=True)
        
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()
    
    @bot.tree.command(name="oc-sync", description="Manually trigger organized crime data sync (Admin only)")
    async def oc_sync(interaction: discord.Interaction):
        """Manually trigger organized crime data sync. Syncs all tracked factions using their respective API keys."""
        if not await require_admin(interaction, "oc-sync", bot):
            return
        
        await interaction.response.defer(ephemeral=True)
        
        if not hasattr(bot, 'database') or not bot.database:
            await interaction.followup.send("❌ Database not available.", ephemeral=True)
            return
        
        try:
            embed = discord.Embed(
                title="⏳ Sync Started",
                color=discord.Color.blue(),
                description="Syncing organized crime data. This may take a moment..."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            
            # Call sync helper
            result = await sync_organized_crimes_helper(bot)
            
            # Build result embed
            result_embed = discord.Embed(
                title="✅ Sync Complete",
                color=discord.Color.green() if result.get("factions_failed", 0) == 0 else discord.Color.orange(),
                description=result.get("message", "Sync completed")
            )
            
            result_embed.add_field(
                name="Factions Synced",
                value=str(result.get("factions_synced", 0)),
                inline=True
            )
            result_embed.add_field(
                name="Crimes Updated",
                value=str(result.get("crimes_updated", 0)),
                inline=True
            )
            result_embed.add_field(
                name="Events Recorded",
                value=str(result.get("events_recorded", 0)),
                inline=True
            )
            
            if result.get("factions_failed", 0) > 0:
                failed_details = result.get("factions_failed_details", [])
                failure_text = []
                for fail in failed_details[:5]:  # Show first 5
                    faction_id = fail.get('faction_id', 'Unknown')
                    reason = fail.get('reason', 'Unknown error')
                    keys_tried = fail.get('keys_tried', 0)
                    
                    # Format error message
                    error_msg = f"Faction {faction_id}: {reason}"
                    if keys_tried > 0:
                        error_msg += f" (tried {keys_tried} key(s))"
                    
                    failure_text.append(error_msg)
                
                result_embed.add_field(
                    name="⚠️ Failed Factions",
                    value="\n\n".join(failure_text) if failure_text else "Unknown errors",
                    inline=False
                )
            
            await interaction.followup.send(embed=result_embed, ephemeral=True)
        
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()
    
    @bot.tree.command(name="oc-list", description="List current organized crimes")
    @discord.app_commands.describe(
        category="Crime status to filter by (optional)"
    )
    async def oc_list(interaction: discord.Interaction, category: Optional[str] = None):
        """List current organized crimes."""
        await interaction.response.defer(ephemeral=False)
        
        if not hasattr(bot, 'database') or not bot.database:
            await interaction.followup.send("❌ Database not available.", ephemeral=True)
            return
        
        try:
            # Get all crimes (no faction filter)
            crimes = await bot.database.get_organized_crimes_current(None)
            
            # Filter by category/status if provided (case-insensitive)
            if category:
                category_lower = category.lower() if isinstance(category, str) else str(category).lower()
                crimes = [c for c in crimes if (c.get('status') or '').lower() == category_lower]
            
            if not crimes:
                await interaction.followup.send(
                    f"No organized crimes found{f' with status {category}' if category else ''}.",
                    ephemeral=True
                )
                return
            
            embed = discord.Embed(
                title="Current Organized Crimes",
                color=discord.Color.blue()
            )
            
            # Group by status
            crimes_by_status = {}
            for crime in crimes:
                status = crime.get('status', 'unknown')
                if status not in crimes_by_status:
                    crimes_by_status[status] = []
                crimes_by_status[status].append(crime)
            
            # Show up to 10 crimes, grouped by status
            shown_count = 0
            for status, status_crimes in list(crimes_by_status.items())[:5]:  # Max 5 status groups
                if shown_count >= 10:
                    break
                
                status_emoji = {
                    'planning': '📋',
                    'ready': '✅',
                    'in_progress': '🔄',
                    'completed': '✔️',
                    'failed': '❌',
                    'cancelled': '🚫'
                }.get(status, '❓')
                
                crime_list = []
                for crime in status_crimes[:10 - shown_count]:  # Max 10 total crimes
                    crime_id = crime['crime_id']
                    crime_url = f"https://www.torn.com/factions.php?step=your&type=1#/tab=crimes&crimeId={crime_id}"
                    if crime['participant_count'] != crime.get('required_participants', 0):
                        # can not be ready without all participants, show planning emoji
                        crime_link = f"📋 **[{crime['crime_name']}]({crime_url})**"
                    else:
                        crime_link = f"{status_emoji} [{crime['crime_name']}]({crime_url})"
                    crime_list.append(
                        f"{crime_link}\n"
                        f"   Participants: {crime['participant_count']}/{crime.get('required_participants', '?')}"
                    )
                    shown_count += 1
                    if shown_count >= 10:
                        break
                
                embed.add_field(
                    name=f"Status: {status.title()}",
                    value="\n".join(crime_list) if crime_list else "No crimes",
                    inline=False
                )
            
            if len(crimes) > 10:
                embed.set_footer(text=f"Showing first 10 of {len(crimes)} crimes")
            
            await interaction.followup.send(embed=embed, ephemeral=False)
        
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=False)
            import traceback
            traceback.print_exc()
    
    @bot.tree.command(name="oc-status", description="Get detailed status of a specific crime")
    @discord.app_commands.describe(
        faction_id="Faction ID",
        crime_id="Crime ID"
    )
    async def oc_status(interaction: discord.Interaction, faction_id: int, crime_id: int):
        """Get detailed status of a specific crime."""
        await interaction.response.defer(ephemeral=True)
        
        if not hasattr(bot, 'database') or not bot.database:
            await interaction.followup.send("❌ Database not available.", ephemeral=True)
            return
        
        try:
            crimes = await bot.database.get_organized_crimes_current(faction_id)
            crime = next((c for c in crimes if c['crime_id'] == crime_id), None)
            
            if not crime:
                await interaction.followup.send(
                    f"❌ Crime {crime_id} not found for faction {faction_id}.",
                    ephemeral=True
                )
                return
            
            embed = discord.Embed(
                title=f"Crime: {crime['crime_name']}",
                color=discord.Color.blue()
            )
            
            embed.add_field(name="Crime ID", value=str(crime['crime_id']), inline=True)
            embed.add_field(name="Faction ID", value=str(crime['faction_id']), inline=True)
            embed.add_field(name="Status", value=crime['status'], inline=True)
            embed.add_field(name="Type", value=crime.get('crime_type', 'Unknown'), inline=True)
            embed.add_field(
                name="Participants",
                value=f"{crime['participant_count']}/{crime.get('required_participants', '?')}",
                inline=True
            )
            
            if crime.get('time_started'):
                time_str = datetime.fromtimestamp(crime['time_started']).strftime("%Y-%m-%d %H:%M:%S")
                embed.add_field(name="Started", value=time_str, inline=True)
            
            if crime.get('time_completed'):
                time_str = datetime.fromtimestamp(crime['time_completed']).strftime("%Y-%m-%d %H:%M:%S")
                embed.add_field(name="Completed", value=time_str, inline=True)
            
            if crime.get('reward_money'):
                embed.add_field(name="Reward Money", value=f"${crime['reward_money']:,}", inline=True)
            
            if crime.get('reward_respect'):
                embed.add_field(name="Reward Respect", value=f"{crime['reward_respect']:,}", inline=True)
            
            participants_str = ", ".join(map(str, crime['participants']))
            if len(participants_str) > 1024:
                participants_str = participants_str[:1021] + "..."
            
            embed.add_field(
                name="Participant IDs",
                value=participants_str if participants_str else "None",
                inline=False
            )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
        
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()
    
    @bot.tree.command(name="oc-stats", description="View participant crime statistics")
    @discord.app_commands.describe(
        player_id="Player ID to get stats for",
        faction_id="Faction ID (optional)",
        crime_type="Crime type filter (optional)"
    )
    async def oc_stats(
        interaction: discord.Interaction,
        player_id: int,
        faction_id: Optional[int] = None,
        crime_type: Optional[str] = None
    ):
        """View participant crime statistics."""
        await interaction.response.defer(ephemeral=True)
        
        if not hasattr(bot, 'database') or not bot.database:
            await interaction.followup.send("❌ Database not available.", ephemeral=True)
            return
        
        try:
            # Get leaves for this player
            if faction_id:
                leaves = await bot.database.get_participant_crime_leaves(faction_id, player_id, 30)
            else:
                # Get all leaves across all factions (limited)
                leaves = []
                # This would require a more complex query, for now just show message
                await interaction.followup.send(
                    "❌ Please specify a faction_id to view stats.",
                    ephemeral=True
                )
                return
            
            # Get current crimes this player is in
            current_crimes = await bot.database.get_organized_crimes_current(faction_id)
            player_crimes = [c for c in current_crimes if player_id in c.get('participants', [])]
            
            embed = discord.Embed(
                title=f"Crime Statistics: Player {player_id}",
                color=discord.Color.blue()
            )
            
            if faction_id:
                embed.add_field(name="Faction ID", value=str(faction_id), inline=True)
            
            embed.add_field(
                name="Current Crimes",
                value=str(len(player_crimes)),
                inline=True
            )
            
            embed.add_field(
                name="Crimes Left (30 days)",
                value=str(len(leaves)),
                inline=True
            )
            
            if leaves:
                recent_leaves = leaves[:5]
                leave_text = []
                for leave in recent_leaves:
                    crime_id = leave.get('crime_id', 'Unknown')
                    timestamp = leave.get('event_timestamp', 0)
                    if timestamp:
                        time_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
                    else:
                        time_str = "Unknown"
                    leave_text.append(f"Crime {crime_id} ({time_str})")
                
                embed.add_field(
                    name="Recent Leaves",
                    value="\n".join(leave_text) if leave_text else "None",
                    inline=False
                )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
        
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()
    
    @bot.tree.command(name="oc-frequent-leavers", description="List players who frequently leave crimes (Faction leads only)")
    @discord.app_commands.describe(
        faction_id="Faction ID",
        threshold="Minimum number of leaves to show (default: 2)",
        days="Time window in days (default: 30)"
    )
    async def oc_frequent_leavers(
        interaction: discord.Interaction,
        faction_id: int,
        threshold: Optional[int] = 2,
        days: Optional[int] = 30
    ):
        """List players who frequently leave crimes."""
        await interaction.response.defer(ephemeral=True)
        
        if not hasattr(bot, 'database') or not bot.database:
            await interaction.followup.send("❌ Database not available.", ephemeral=True)
            return
        
        try:
            # Check if user is faction lead or admin
            config = await bot.database.get_organized_crime_config(
                faction_id, str(interaction.guild.id)
            )
            
            if not config:
                await interaction.followup.send(
                    f"❌ No organized crime tracking configured for faction {faction_id} in this server.",
                    ephemeral=True
                )
                return
            
            lead_ids = config.get('faction_lead_discord_ids', [])
            user_id_str = str(interaction.user.id)
            is_admin = interaction.user.guild_permissions.administrator
            is_lead = user_id_str in lead_ids
            
            if not (is_admin or is_lead):
                await interaction.followup.send(
                    "❌ You must be a faction lead or server administrator to view frequent leavers.",
                    ephemeral=True
                )
                return
            
            leavers = await bot.database.get_frequent_leavers(faction_id, threshold or 2, days or 30)
            
            if not leavers:
                await interaction.followup.send(
                    f"No frequent leavers found (threshold: {threshold or 2}, window: {days or 30} days).",
                    ephemeral=True
                )
                return
            
            embed = discord.Embed(
                title="Frequent Crime Leavers",
                color=discord.Color.orange(),
                description=f"Players who left more than {threshold or 2} crimes in the last {days or 30} days"
            )
            
            leaver_texts = []
            for leaver in leavers[:20]:  # Max 20
                leaver_texts.append(
                    f"**Player {leaver['player_id']}** - {leaver['leave_count']} leaves"
                )
            
            embed.description = "\n".join(leaver_texts)
            
            if len(leavers) > 20:
                embed.set_footer(text=f"Showing first 20 of {len(leavers)} leavers")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
        
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            import traceback
            traceback.print_exc()

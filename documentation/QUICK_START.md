# Quick Start Guide - Staging Deployment

## First-Time Setup

### 1. Portainer Setup (One Time)

**⚠️ If you get "compose file not found" error, use Web Editor method below**

#### Method A: Web Editor (Recommended - Works Immediately)
1. Go to `https://192.168.0.54:9443/`
2. Stacks → Add Stack → Name: `jab-discord-staging`
3. **Select "Web editor"** (not Repository)
4. Copy the contents of `docker-compose.yml` from your local repository
5. Paste into Portainer's web editor
6. Add environment variables:
   - `DISCORD_BOT_TOKEN` = Your bot token
   - `DISCORD_GUILD_ID` = (Optional) Your guild ID
   - `WEB_PORT` = `5000`
   - `FLASK_DEBUG` = `False`
7. Click **Deploy the stack**

#### Method B: Git Repository (For Auto-Updates)
1. Go to `https://192.168.0.54:9443/`
2. Stacks → Add Stack → Name: `jab-discord-staging`
3. **Select "Repository"**
4. Repository URL: Your GitHub repo (use `https://username:token@github.com/user/repo.git` for private repos)
5. Reference: `main`
6. Compose path: `docker-compose.yml` (no leading slash, no ./ prefix)
7. Add environment variables:
   - `DISCORD_BOT_TOKEN` = Your bot token
   - `DISCORD_GUILD_ID` = (Optional) Guild ID
   - `WEB_PORT` = `5000`
   - `FLASK_DEBUG` = `False`
7. Deploy the stack

### 2. Verify Deployment
- Check containers are running in Portainer
- Check logs for errors
- Test web app: `http://192.168.0.54:5000`
- Verify bot is online in Discord

## Daily Usage

### Deploy Updates

**Method 1: Using Script**
```bash
./scripts/deploy.sh
```

**Method 2: Manual**
```bash
git add .
git commit -m "Your changes"
git push origin main
# Then update stack in Portainer
```

### Update in Portainer
1. Stacks → `jab-discord-staging`
2. Click **Editor** or **Update the stack**
3. Click **Pull and redeploy**

### View Logs
- Portainer → Containers → Select container → Logs

### Backup Database
```bash
./scripts/backup.sh
```

### Check Deployment Status
```bash
./scripts/check-deployment.sh
```

## Quick Commands

| Action | Command |
|--------|---------|
| Deploy changes | `./scripts/deploy.sh` |
| Backup | `./scripts/backup.sh` |
| Check status | `./scripts/check-deployment.sh` |
| View Portainer | Open `https://192.168.0.54:9443/` |
| Access Web App | Open `http://192.168.0.54:5000` |

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Container won't start | Check logs in Portainer |
| Bot not connecting | Verify `DISCORD_BOT_TOKEN` in Portainer |
| Web app not accessible | Check port mapping and firewall |
| Database errors | Check volume permissions and disk space |

## Files Overview

- `docker-compose.yml` - Container orchestration
- `Dockerfile` - Container image definition
- `env.example` - Environment variables template
- `DEPLOYMENT.md` - Detailed deployment guide
- `documentation/DEPLOYMENT_PLAN.md` - Complete deployment plan
- `scripts/deploy.sh` - Deployment helper script
- `scripts/backup.sh` - Backup helper script
- `scripts/check-deployment.sh` - Status check script

## Environment Variables

Required:
- `DISCORD_BOT_TOKEN` - Your Discord bot token

Optional:
- `DISCORD_GUILD_ID` - Guild ID for testing
- `WEB_PORT` - Web app port (default: 5000)
- `FLASK_DEBUG` - Debug mode (default: False)
- `DATABASE_PATH` - Database path (default: data/torn_data.db)

Set these in Portainer: Stacks → Environment Variables

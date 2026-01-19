# Deployment Guide

This guide walks you through deploying the Discord bot to your staging environment on Ubuntu server with Portainer.

## Quick Start

1. **Set up Portainer Stack** (first time only)
2. **Configure Environment Variables** in Portainer
3. **Deploy from GitHub** using Portainer
4. **Verify Deployment** and monitor logs

## Prerequisites

- Ubuntu server running at `192.168.0.54` with Portainer installed
- GitHub repository with your code
- Discord bot token for staging environment
- Access to Portainer UI at `https://192.168.0.54:9443/`

## Step-by-Step Deployment

### 1. Initial Portainer Setup

#### Access Portainer
1. Open your browser and navigate to `https://192.168.0.54:9443/`
2. Log in to Portainer

#### Create a New Stack
1. Navigate to **Stacks** in the left sidebar
2. Click **Add Stack**
3. Name the stack: `jab-discord-staging`

#### Configure Git Repository
1. Select **Repository** as the build method
2. Enter your **Repository URL** (e.g., `https://github.com/yourusername/jab-discord.git`)
   - If the repository is private, use: `https://username:token@github.com/yourusername/jab-discord.git`
   - Or use SSH: `git@github.com:yourusername/jab-discord.git` (requires SSH key setup in Portainer)
3. Set **Reference**: `main` (or your default branch)
4. Set **Compose path**: `docker-compose.yml` (must be relative to repository root)
5. Enable **Auto-update** if you want automatic deployments on push

**Alternative: Web Editor Method** (if Git method fails)
1. Select **Web editor** as the build method
2. Copy the contents of `docker-compose.yml` from your local repository
3. Paste it into the Portainer web editor
4. This method requires manual updates but works immediately

#### Set Environment Variables
Click **Environment variables** and add the following:

| Variable | Description | Example |
|----------|-------------|---------|
| `DISCORD_BOT_TOKEN` | Your Discord bot token | `MTxxxxxxxxxxxxx` |
| `DISCORD_GUILD_ID` | (Optional) Guild ID for testing | `123456789012345` |
| `WEB_PORT` | (Optional) Web app port | `5000` |
| `FLASK_DEBUG` | (Optional) Debug mode | `False` |

**⚠️ Important:** Never commit your `.env` file or expose tokens in code.

### 2. Deploy the Stack

1. Review your configuration
2. Click **Deploy the stack**
3. Wait for Portainer to pull from GitHub and build the containers
4. Monitor the deployment logs

### 3. Verify Deployment

#### Check Container Status
1. In Portainer, go to **Containers**
2. Verify both `jab-discord-bot` and `jab-discord-webapp` are running
3. Check their status shows as "Healthy"

#### Check Logs
1. Click on a container name
2. Go to **Logs** tab
3. Verify no errors in the logs
4. Look for "Bot has logged in!" in bot logs
5. Look for "Starting web interface" in webapp logs

#### Test the Web App
1. Open `http://192.168.0.54:5000` (or your configured port) in a browser
2. Verify the database browser loads
3. Test the API endpoint: `http://192.168.0.54:5000/api/tables`

#### Test the Bot
1. Check Discord to verify bot is online
2. Test a bot command in your Discord server
3. Verify command responses work correctly

## Updating the Deployment

### Method 1: Manual Update (Recommended)

1. **Push changes to GitHub:**
   ```bash
   git add .
   git commit -m "Your commit message"
   git push origin main
   ```
   
   Or use the provided script:
   ```bash
   ./scripts/deploy.sh
   ```

2. **Update in Portainer:**
   - Go to **Stacks** → `jab-discord-staging`
   - Click **Editor** or **Update the stack**
   - Click **Pull and redeploy** to fetch latest from GitHub
   - Containers will automatically rebuild and restart

### Method 2: Automatic Update

If you enabled auto-update:
1. Push changes to GitHub
2. Portainer will automatically detect changes
3. Containers will rebuild and restart automatically
4. Monitor logs to verify deployment

## Monitoring

### View Logs
- **Real-time logs**: Container → Logs tab
- **Historical logs**: Logs are retained per container settings
- **Log rotation**: Configured to keep 3 files of 10MB each

### Health Checks
- Containers include health checks
- Portainer displays health status
- Check container status if issues occur

### Database Status
- Database is persisted in Docker volume
- Check disk space: Portainer → Volumes → `jab-discord-data`
- Use backup script before major updates

## Troubleshooting

### Portainer Can't Find Compose File

**Error**: `failed to load the compose file: no such file or directory`

**Solutions**:

1. **Verify Repository Access**
   - Ensure Portainer can access your GitHub repository
   - For private repos, use authentication in the URL format: `https://username:token@github.com/user/repo.git`
   - Or configure SSH keys in Portainer for SSH-based access

2. **Check Compose File Path**
   - The path should be relative to the repository root
   - If `docker-compose.yml` is in the root, use: `docker-compose.yml`
   - Don't use leading slash: `/docker-compose.yml` ❌
   - Don't use `./docker-compose.yml` ❌
   - Use: `docker-compose.yml` ✅

3. **Verify File Exists in Repository**
   - Check that `docker-compose.yml` is committed and pushed to GitHub
   - Verify the file is on the branch you specified (usually `main`)

4. **Use Web Editor Method Instead**
   - In Portainer: Stacks → Add Stack → **Web editor** (instead of Repository)
   - Copy the contents of your local `docker-compose.yml`
   - Paste into Portainer's web editor
   - This bypasses Git access issues

5. **Manual Repository Clone** (Advanced)
   - SSH into your Ubuntu server
   - Clone the repository manually
   - Use **Upload** method in Portainer to upload the compose file

6. **Check Portainer Logs**
   - Portainer → Settings → About → View logs
   - Look for Git-related errors or authentication issues

### Container Won't Start

1. **Check logs:**
   - Container → Logs tab
   - Look for error messages

2. **Verify environment variables:**
   - Stack → Environment variables
   - Ensure all required variables are set
   - Check for typos

3. **Check resource usage:**
   - Container → Stats
   - Ensure enough memory/CPU available

### Bot Not Connecting

1. **Verify Discord token:**
   - Check token in Portainer environment variables
   - Ensure token is valid and hasn't been revoked
   - Try regenerating token in Discord Developer Portal

2. **Check network connectivity:**
   - Ensure server can reach Discord API
   - Check firewall rules
   - Review bot logs for connection errors

### Web App Not Accessible

1. **Check port mapping:**
   - Verify port is mapped correctly in docker-compose.yml
   - Check if port is already in use
   - Ensure firewall allows access to the port

2. **Check container logs:**
   - Look for Flask startup errors
   - Verify database connection
   - Check for permission errors

### Database Issues

1. **Check volume:**
   - Portainer → Volumes → `jab-discord-data`
   - Verify volume exists and has data
   - Check disk space

2. **Verify permissions:**
   - Container may need write access to data directory
   - Check volume mount configuration

### Rollback

If something goes wrong:

1. **Quick rollback:**
   - Stack → Editor
   - Revert to previous Git commit
   - Update stack

2. **Stop containers:**
   - Containers → Select containers → Stop
   - Fix issues locally
   - Redeploy when ready

## Backup and Recovery

### Backup Database

1. **Using Portainer:**
   - Volumes → `jab-discord-data` → Backup
   - Download backup file

2. **Using SSH:**
   ```bash
   ssh user@192.168.0.54
   docker run --rm -v jab-discord-data:/data -v $(pwd):/backup \
     ubuntu tar czf /backup/jab-discord-backup-$(date +%Y%m%d).tar.gz /data
   ```

3. **Using backup script:**
   ```bash
   ./scripts/backup.sh
   ```

### Restore Database

1. **Using Portainer:**
   - Stop containers
   - Volumes → Restore from backup
   - Start containers

2. **Using SSH:**
   ```bash
   docker run --rm -v jab-discord-data:/data -v $(pwd):/backup \
     ubuntu tar xzf /backup/jab-discord-backup-YYYYMMDD.tar.gz -C /
   ```

## Security Best Practices

1. **Environment Variables:**
   - Never commit `.env` files
   - Use Portainer's environment variable management
   - Rotate tokens periodically

2. **Network Security:**
   - Web app only accessible on local network
   - Use firewall rules if needed
   - Keep Portainer updated

3. **Access Control:**
   - Limit Portainer access to trusted users
   - Use strong passwords
   - Enable 2FA if available

## Maintenance

### Regular Tasks

- **Daily**: Check container status and logs
- **Weekly**: Review error logs
- **Monthly**: Update dependencies
- **As needed**: Backup database before major updates

### Update Dependencies

1. Update `requirements.txt` locally
2. Test locally
3. Commit and push changes
4. Update stack in Portainer

## Support

For issues:
1. Check container logs in Portainer
2. Review deployment documentation
3. Check GitHub issues
4. Verify environment configuration

## Additional Resources

- [Portainer Documentation](https://docs.portainer.io/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Discord.py Documentation](https://discordpy.readthedocs.io/)

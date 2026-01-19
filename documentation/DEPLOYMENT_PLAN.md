# Deployment Plan: Development to Staging

This document outlines the deployment process for moving the Discord bot from local development to a staging environment on your Ubuntu server with Portainer.

## Architecture Overview

### Development Environment (Local Machine)
- **Location**: Your local computer
- **Purpose**: Active development and testing
- **Run Method**: Direct Python execution (`python bot.py`)
- **Code Management**: Git commits and pushes to GitHub

### Staging Environment (Ubuntu Server)
- **Location**: Ubuntu server at `192.168.0.54`
- **Purpose**: Testing with friends, always-on availability
- **Run Method**: Docker containers managed by Portainer
- **Code Management**: Automatic deployment from GitHub

## Environment Separation

### Development Environment Variables
- `.env` file on your local machine
- Contains development/test tokens if needed
- Never committed to Git

### Staging Environment Variables
- Configured in Portainer stack environment
- Separate Discord bot token for staging
- Production-ready configuration

## Deployment Workflow

### 1. Development Workflow
```
Local Machine → Git Commit → Git Push → GitHub
```

### 2. Deployment to Staging
```
GitHub → Docker Build → Portainer Stack → Running Container
```

### 3. Update Process
```
1. Make changes locally
2. Test changes locally
3. Commit and push to GitHub
4. Trigger deployment on Portainer (manual or automatic)
5. Verify staging environment
```

## Portainer Setup

### Initial Setup Steps

1. **Access Portainer**
   - URL: `https://192.168.0.54:9443/`
   - Log in to Portainer

2. **Create a Stack**
   - Go to Stacks → Add Stack
   - Name: `jab-discord-staging`
   - Build Method: Git Repository

3. **Configure Git Repository**
   - Repository URL: Your GitHub repository URL
   - Reference: `main` (or your default branch)
   - Compose File Path: `docker-compose.yml`

4. **Set Environment Variables**
   - Configure all required environment variables in Portainer
   - Use Portainer's environment variable management
   - Never commit secrets to Git

## Required Environment Variables

### Bot Configuration
- `DISCORD_BOT_TOKEN`: Discord bot token for staging
- `DISCORD_GUILD_ID`: (Optional) Specific guild ID for testing

### Database Configuration
- `DATABASE_PATH`: (Optional) Defaults to `data/torn_data.db`

### Web App Configuration
- `WEB_PORT`: (Optional) Defaults to `5000`
- `FLASK_DEBUG`: (Optional) Set to `False` for staging

## Docker Configuration

### Container Structure
- **Bot Container**: Runs the Discord bot
- **Web App Container** (Optional): Runs the Flask web interface
- **Volume Mounts**: Persistent storage for database files

### Port Mappings
- Web App: `5000:5000` (or your configured port)
- Bot: No exposed ports needed

## Database Persistence

### Volume Configuration
- Database files stored in Docker volume: `jab-discord-data`
- Persistent across container restarts
- Backups recommended before major updates

## Update Process

### Manual Update
1. Commit changes to GitHub
2. In Portainer: Stacks → jab-discord-staging → Editor
3. Update the stack (re-pull from Git)
4. Restart containers

### Automatic Update (Optional)
- Configure webhook in GitHub
- Portainer receives webhook on push
- Automatic rebuild and redeploy

## Monitoring and Logs

### Portainer Logs
- View container logs in Portainer UI
- Real-time log streaming
- Log retention settings

### Health Checks
- Bot heartbeat in database
- Web app health endpoint
- Container status monitoring

## Rollback Procedure

### Quick Rollback
1. In Portainer, go to Stack editor
2. Revert to previous commit in Git
3. Update stack to pull previous version
4. Restart containers

### Emergency Stop
1. Stop containers in Portainer
2. Fix issues locally
3. Test locally
4. Redeploy to staging

## Security Considerations

### Environment Variables
- Never commit `.env` files
- Use Portainer secrets management
- Rotate tokens periodically

### Network Security
- Web app only accessible on local network
- Consider firewall rules if needed
- Use HTTPS for Portainer access

### Access Control
- Limit Portainer access to trusted users
- Use strong passwords
- Consider 2FA if available

## Backup Strategy

### Database Backups
- Regular backups of `data/` directory
- Store backups outside container
- Automated backup script (see `scripts/backup.sh`)

### Configuration Backups
- Backup Portainer stack configuration
- Export environment variables securely
- Document any manual configurations

## Troubleshooting

### Common Issues

1. **Container won't start**
   - Check environment variables
   - Verify GitHub repository access
   - Check Portainer logs

2. **Bot not connecting**
   - Verify Discord token
   - Check network connectivity
   - Review bot logs

3. **Database errors**
   - Check volume permissions
   - Verify database path
   - Check disk space

4. **Web app not accessible**
   - Verify port mapping
   - Check firewall rules
   - Review container logs

### Support Resources
- Portainer documentation
- Docker logs
- GitHub repository issues

## Next Steps

1. Set up Portainer stack using `docker-compose.yml`
2. Configure environment variables
3. Test initial deployment
4. Set up monitoring and alerts
5. Document any custom configurations

## Maintenance Schedule

- **Daily**: Check container status
- **Weekly**: Review logs for errors
- **Monthly**: Update dependencies
- **As needed**: Backup database

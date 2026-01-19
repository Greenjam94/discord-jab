# Portainer Setup Troubleshooting Guide

## Error: "Failed to deploy a stack: failed to load the compose file"

This error means Portainer cannot find or access your `docker-compose.yml` file.

## Quick Fix: Use Web Editor Method

The fastest solution is to use Portainer's web editor instead of Git repository mode:

### Steps:

1. **In Portainer:**
   - Go to **Stacks** → **Add Stack**
   - Name: `jab-discord-staging`
   - **IMPORTANT**: Select **"Web editor"** (not "Repository")

2. **Copy Docker Compose Content:**
   - On your local machine, open `docker-compose.yml`
   - Copy the entire contents (Ctrl+A, Ctrl+C)

3. **Paste in Portainer:**
   - Paste the contents into the Portainer web editor
   - Review to ensure it copied correctly

4. **Add Environment Variables:**
   - Scroll down to **Environment variables**
   - Add these variables:
     ```
     DISCORD_BOT_TOKEN=your_bot_token_here
     DISCORD_GUILD_ID=your_guild_id_here (optional)
     WEB_PORT=5000 (optional)
     FLASK_DEBUG=False (optional)
     ```

5. **Deploy:**
   - Click **Deploy the stack**

### Updating Later with Web Editor Method:

1. Make changes locally
2. Push to GitHub (for version control)
3. In Portainer: Stacks → `jab-discord-staging` → **Editor**
4. Paste updated `docker-compose.yml` content
5. Click **Update the stack**

---

## Fixing Git Repository Method

If you want to use Git repository method (for auto-updates):

### Issue 1: Private Repository Access

**Problem**: Portainer can't access your private GitHub repository.

**Solution A: Use Personal Access Token**
1. Create a GitHub Personal Access Token:
   - GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
   - Generate new token with `repo` scope
   - Copy the token

2. Use token in repository URL:
   ```
   https://YOUR_USERNAME:YOUR_TOKEN@github.com/YOUR_USERNAME/jab-discord.git
   ```

**Solution B: Use SSH**
1. Generate SSH key on Ubuntu server:
   ```bash
   ssh-keygen -t ed25519 -C "portainer@your-server"
   ```
2. Add public key to GitHub (Settings → SSH and GPG keys)
3. Test SSH connection
4. Use SSH URL in Portainer:
   ```
   git@github.com:YOUR_USERNAME/jab-discord.git
   ```

### Issue 2: Wrong Compose Path

**Problem**: Portainer can't find the file at the specified path.

**Solution**:
- ✅ Correct: `docker-compose.yml`
- ❌ Wrong: `/docker-compose.yml`
- ❌ Wrong: `./docker-compose.yml`
- ❌ Wrong: `compose/docker-compose.yml` (unless file is actually there)

**Verify**:
1. Check your GitHub repository
2. Ensure `docker-compose.yml` is in the root directory
3. Ensure it's committed and pushed to the branch you specified

### Issue 3: Branch Name Mismatch

**Problem**: Specified branch doesn't exist or doesn't have the file.

**Solution**:
- Verify your default branch name (might be `main`, `master`, or another name)
- Check: `git branch -a` on your local machine
- Use the correct branch name in Portainer's "Reference" field

### Issue 4: Portainer Can't Clone Repository

**Problem**: Network or permission issues preventing clone.

**Solution**:
1. **Test from server:**
   ```bash
   ssh user@192.168.0.54
   git clone https://github.com/YOUR_USERNAME/jab-discord.git /tmp/test-clone
   ```
   
2. **Check Portainer network settings:**
   - Portainer → Settings → Settings
   - Verify network configuration
   - Check if firewall is blocking GitHub

3. **Check Portainer logs:**
   - Portainer → Settings → About
   - View logs for Git-related errors

---

## Alternative: Upload Method

If both Git and Web editor methods fail:

1. **On your local machine:**
   - Ensure `docker-compose.yml` is ready
   - You can compress it (though not required)

2. **In Portainer:**
   - Stacks → Add Stack
   - Select **Upload** method
   - Upload your `docker-compose.yml` file
   - Add environment variables
   - Deploy

**Note**: This method requires manual file uploads for each update.

---

## Verification Checklist

Before trying to deploy, verify:

- [ ] `docker-compose.yml` exists in your repository root
- [ ] File is committed to Git: `git status` shows no uncommitted changes
- [ ] File is pushed to GitHub: `git push` succeeds
- [ ] File is on the branch you're specifying in Portainer
- [ ] If repository is private, you've configured authentication
- [ ] Portainer can reach GitHub (test from server if needed)

---

## Testing Repository Access

SSH into your server and test:

```bash
# Test HTTPS access
git clone https://github.com/YOUR_USERNAME/jab-discord.git /tmp/test

# Test with token (replace YOUR_TOKEN)
git clone https://YOUR_TOKEN@github.com/YOUR_USERNAME/jab-discord.git /tmp/test2

# Verify compose file exists
ls -la /tmp/test/docker-compose.yml

# Clean up
rm -rf /tmp/test /tmp/test2
```

If these commands work, Portainer should be able to access the repository too.

---

## Still Having Issues?

1. **Check Portainer version:**
   - Older versions might have Git integration issues
   - Consider updating Portainer

2. **Use Web Editor as Temporary Solution:**
   - Get it working first with web editor
   - Troubleshoot Git method separately

3. **Check Portainer documentation:**
   - https://docs.portainer.io/user/stacks/add/

4. **Check server resources:**
   - Ensure server has enough disk space
   - Check Docker is running: `docker ps`

---

## Recommended Approach

For **getting started quickly**:
1. ✅ Use **Web editor** method (fastest, works immediately)
2. Set up Git repository method later for automation

For **production/automation**:
1. Use **Repository** method with proper authentication
2. Enable auto-update for seamless deployments

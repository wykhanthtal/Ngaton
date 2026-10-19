# Ngaton

Telegram scanner bot whose redeem-code database is `redeem_codes.json`
stored in a GitHub repo (read/written via the GitHub API) — no SQLite,
so codes survive restarts and redeploys.

Required environment variables (set in Railway → Variables; never in files):
- BOT_TOKEN     Telegram bot token from @BotFather
- GITHUB_TOKEN  GitHub fine-grained token, Contents: Read and write,
                limited to the REPO_NAME repository only
- REPO_NAME     Repo that stores redeem_codes.json (e.g. this repo)
- REPO_OWNER    Defaults to wykhanthtal
- ADMIN_ID      Defaults to 5115562507

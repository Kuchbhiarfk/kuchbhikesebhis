import requests
import json
import time
import os
import asyncio
import threading
import uuid
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
import random
import string
from pymongo import MongoClient
from pymongo.errors import ConnectionError

# MongoDB configuration
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://")  # Replace with your MongoDB URI
DB_NAME = "Cluster0"  # Replace with your database name
COLLECTION_NAME = "render_accounts"

# Initialize MongoDB client
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = mongo_client[DB_NAME]
    accounts_collection = db[COLLECTION_NAME]
    # Test connection
    mongo_client.server_info()
except ConnectionError as e:
    print(f"Failed to connect to MongoDB: {e}")
    raise

# Load accounts from MongoDB
def load_accounts():
    try:
        accounts = list(accounts_collection.find({}))
        # Remove MongoDB's '_id' field from each document to match previous JSON structure
        for acc in accounts:
            acc.pop("_id", None)
        return accounts
    except Exception as e:
        print(f"Error loading accounts from MongoDB: {e}")
        return []

# Save accounts to MongoDB
def save_accounts(accounts):
    try:
        # Clear existing accounts and insert new ones to ensure consistency
        accounts_collection.delete_many({})
        if accounts:
            accounts_collection.insert_many(accounts)
    except Exception as e:
        print(f"Error saving accounts to MongoDB: {e}")

# Generate random name for deployment
def generate_random_name():
    return "txesc-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))

# Generate random UUID for render-request-id
def generate_request_id():
    return str(uuid.uuid4())

# Step 1: Sign-in to Render
async def sign_in(email, password):
    signin_url = "https://api.render.com/graphql"
    signin_headers = {
        "accept": "*/*",
        "content-type": "application/json",
        "render-request-id": generate_request_id()
    }
    signin_data = {
        "operationName": "signIn",
        "variables": {
            "email": email,
            "password": password
        },
        "query": """mutation signIn($email: String!, $password: String!) {
  signIn(email: $email, password: $password) {
    ...authResultFields
    __typename
  }
}
fragment authResultFields on AuthResult {
  idToken
  expiresAt
  user {
    ...userFields
    sudoModeExpiresAt
    __typename
  }
  readOnly
  workspaces {
    ...workspaceFields
    __typename
  }
  __typename
}
fragment userFields on User {
  id
  active
  bitbucketId
  createdAt
  email
  featureFlags
  githubId
  gitlabId
  googleId
  name
  notifyOnPrUpdate
  otpEnabled
  passwordExists
  tosAcceptedAt
  intercomEmailHMAC
  themeSetting
  avatarDownload {
    url
    __typename
  }
  __typename
}
fragment workspaceFields on Workspace {
  email
  id
  name
  tier
  avatarDownload {
    url
    __typename
  }
  __typename
}"""
    }
    response = requests.post(signin_url, headers=signin_headers, json=signin_data)
    if response.status_code != 200:
        return None, f"Sign-in failed: {response.status_code} - {response.text}"
    signin_json = response.json()
    if "errors" in signin_json:
        return None, f"Sign-in error: {signin_json['errors']}"
    id_token = signin_json["data"]["signIn"]["idToken"]
    owner_id = signin_json["data"]["signIn"]["workspaces"][0]["id"]
    return id_token, owner_id

# Step 2: Create server on Render
async def create_server(id_token, owner_id, github_url, bot_token, op_command, nonop_command, random_name):
    create_server_url = "https://api.render.com/graphql"
    create_server_headers = {
        "accept": "*/*",
        "content-type": "application/json",
        "authorization": f"Bearer {id_token}",
        "render-request-id": generate_request_id()
    }
    create_server_data = {
        "operationName": "createServer",
        "variables": {
            "server": {
                "autoDeployTrigger": "push",
                "baseDir": "",
                "branch": "main",
                "buildCommand": "",
                "name": random_name,
                "dockerfilePath": "",
                "dockerCommand": "",
                "envId": "docker",
                "envVars": [
                    {"key": "BOT_TOKEN", "value": bot_token, "isFile": False},
                    {"key": "OP_COMMAND", "value": op_command, "isFile": False},
                    {"key": "NONOP_COMMAND", "value": nonop_command, "isFile": False}
                ],
                "healthCheckPath": "",
                "ownerId": owner_id,
                "plan": "Free",
                "repo": {
                    "name": "txesc",
                    "ownerName": "nahiefhspc",
                    "webURL": github_url,
                    "isFork": False,
                    "isPrivate": False,
                    "provider": "GITHUB",
                    "providerId": "988790559",
                    "defaultBranchName": "main"
                },
                "externalImage": None,
                "isWorker": False,
                "isPrivate": False,
                "region": "singapore",
                "startCommand": "",
                "staticPublishPath": "",
                "rootDir": "",
                "buildFilter": {"paths": [], "ignoredPaths": []},
                "preDeployCommand": None,
                "environmentId": None,
                "registryCredentialId": None
            }
        },
        "query": """mutation createServer($server: ServerInput!) {
  createServer(server: $server) {
    ...serverFields
    __typename
  }
}
fragment serverFields on Server {
  ...serviceFields
  autoDeployTrigger
  autoscalingConfig {
    enabled
    min
    max
    cpuPercentage
    cpuEnabled
    memoryPercentage
    memoryEnabled
    __typename
  }
  deploy {
    ...deployFields
    __typename
  }
  deployKey
  externalImage {
    ...externalImageFields
    __typename
  }
  extraInstances
  healthCheckHost
  healthCheckPath
  isPrivate
  isWorker
  openPorts
  maintenanceScheduledAt
  pendingMaintenanceBy
  cacheProfile
  permissions {
    accessShell { ...permissionResultFields __typename }
    deleteCustomDomain { ...permissionResultFields __typename }
    deleteServer { ...permissionResultFields __typename }
    deleteServerDisk { ...permissionResultFields __typename }
    deployServer { ...permissionResultFields __typename }
    manageStaticSiteConfiguration { ...permissionResultFields __typename }
    restoreServerDiskSnapshot { ...permissionResultFields __typename }
    resumeServer { ...permissionResultFields __typename }
    suspendServer { ...permissionResultFields __typename }
    updateMaintenanceMode { ...permissionResultFields __typename }
    updateServer { ...permissionResultFields __typename }
    updateServerIPAllowList { ...permissionResultFields __typename }
    viewServerIPAllowList { ...permissionResultFields __typename }
    viewServerEvents { ...permissionResultFields __typename }
    viewServerMetrics { ...permissionResultFields __typename }
    __typename
  }
  plan {
    name
    cpu
    mem
    price
    __typename
  }
  previewGeneration
  preDeployCommand
  pullRequestId
  pullRequest {
    id
    number
    url
    __typename
  }
  rootDir
  startCommand
  staticPublishPath
  suspenders
  url
  disk {
    ...diskFields
    __typename
  }
  maintenance {
    id
    type
    scheduledAt
    pendingMaintenanceBy
    state
    __typename
  }
  maintenanceMode {
    enabled
    uri
    __typename
  }
  renderSubdomainPolicy
  __typename
}
fragment serviceFields on Service {
  id
  type
  env {
    ...envFields
    __typename
  }
  repo {
    ...repoFields
    __typename
  }
  user {
    id
    email
    __typename
  }
  ownerId
  owner {
    id
    email
    billingStatus
    featureFlags
    __typename
  }
  name
  slug
  sourceBranch
  buildCommand
  buildFilter {
    paths
    ignoredPaths
    __typename
  }
  buildPlan {
    name
    cpu
    mem
    __typename
  }
  externalImage {
    ...externalImageFields
    __typename
  }
  autoDeploy
  userFacingType
  userFacingTypeSlug
  baseDir
  dockerCommand
  dockerfilePath
  createdAt
  updatedAt
  outboundIPs
  region {
    id
    description
    __typename
  }
  registryCredential {
    id
    name
    __typename
  }
  rootDir
  shellURL
  state
  suspenders
  sshAddress
  sshServiceAvailable
  lastDeployedAt
  maintenanceScheduledAt
  pendingMaintenanceBy
  environment {
    ...environmentFields
    __typename
  }
  iacExecutionSource {
    id
    name
    lastSyncAt
    repo {
      name
      __typename
    }
    __typename
  }
  __typename
}
fragment envFields on Env {
  id
  name
  language
  isStatic
  sampleBuildCommand
  sampleStartCommand
  __typename
}
fragment environmentFields on Environment {
  id
  name
  isolated
  protected
  project {
    id
    name
    owner {
      id
      __typename
    }
    __typename
  }
  permissions {
    manageEnvironmentSecrets { ...permissionResultFields __typename }
    moveEnvironmentResources { ...permissionResultFields __typename }
    __typename
  }
  __typename
}
fragment permissionResultFields on PermissionResult {
  permissionLevel
  message
  __typename
}
fragment repoFields on Repo {
  id
  provider
  providerId
  name
  ownerName
  webURL
  isPrivate
  __typename
}
fragment externalImageFields on ExternalImage {
  imageHost
  imageName
  imageRef
  imageRepository
  imageURL
  ownerId
  registryCredentialId
  __typename
}
fragment deployFields on Deploy {
  id
  status
  buildId
  commitId
  commitShortId
  commitMessage
  commitURL
  commitCreatedAt
  finishedAt
  finishedAtUnixNano
  initialDeployHookFinishedAtUnixNano
  createdAt
  updatedAt
  server {
    id
    userFacingTypeSlug
    __typename
  }
  rollbackSupportStatus
  reason {
    ...failureReasonFields
    __typename
  }
  imageSHA
  externalImage {
    imageRef
    __typename
  }
  __typename
}
fragment failureReasonFields on FailureReason {
  badStartCommand
  evicted
  evictionReason
  earlyExit
  nonZeroExit
  oomKilled {
    memoryLimit
    __typename
  }
  rootDirMissing
  step
  timedOutSeconds
  timedOutReason
  unhealthy
  __typename
}
fragment diskFields on Disk {
  id
  name
  mountPath
  sizeGB
  __typename
}"""
    }
    response = requests.post(create_server_url, headers=create_server_headers, json=create_server_data)
    if response.status_code != 200:
        return None, None, f"Create server failed: {response.status_code} - {response.text}"
    create_server_json = response.json()
    if "errors" in create_server_json:
        return None, None, f"Create server error: {create_server_json['errors']}"
    srv_id = create_server_json["data"]["createServer"]["id"]
    server_url = create_server_json["data"]["createServer"]["url"]
    return srv_id, server_url, None

# Step 3: Poll deployment status
async def poll_status(id_token, srv_id, update, context, message_id, random_name, server_url, email, password, display_name, bot_token, op_command, nonop_command):
    status_url = "https://api.render.com/graphql"
    status_headers = {
        "accept": "*/*",
        "content-type": "application/json",
        "authorization": f"Bearer {id_token}",
        "render-request-id": generate_request_id()
    }
    status_data = {
        "operationName": "statusQuery",
        "variables": {"id": srv_id},
        "query": """query statusQuery($id: String!) {
  status(id: $id) {
    label
    state
    __typename
  }
}"""
    }
    start_time = time.time()
    timeout = 15 * 60  # 5 minutes in seconds
    while time.time() - start_time < timeout:
        response = requests.post(status_url, headers=status_headers, json=status_data)
        addon_on = True  # Deployment is active during polling
        if response.status_code != 200:
            addon_on = False
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=message_id,
                text=f"Name - {random_name}\n"
                     f"Server Url - {server_url}\n"
                     f"Deploy Status - Failed\n"
                     f"Addon On - {addon_on}\n"
                     f"Display Name - {display_name}\n"
                     f"Email - {email}\n"
                     f"Password - {password}\n"
                     f"BOT_TOKEN - {bot_token}\n"
                     f"OP_COMMAND - {op_command}\n"
                     f"NONOP_COMMAND - {nonop_command}\n"
                     f"Status request failed: {response.status_code} - {response.text}"
            )
            return
        status_json = response.json()
        if "errors" in status_json:
            addon_on = False
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=message_id,
                text=f"Name - {random_name}\n"
                     f"Server Url - {server_url}\n"
                     f"Deploy Status - Failed\n"
                     f"Addon On - {addon_on}\n"
                     f"Display Name - {display_name}\n"
                     f"Email - {email}\n"
                     f"Password - {password}\n"
                     f"BOT_TOKEN - {bot_token}\n"
                     f"OP_COMMAND - {op_command}\n"
                     f"NONOP_COMMAND - {nonop_command}\n"
                     f"Status error: {status_json['errors']}"
            )
            return
        status_state = status_json["data"]["status"]["state"]
        status_label = status_json["data"]["status"]["label"]
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=message_id,
            text=f"Name - {random_name}\n"
                 f"Server Url - {server_url}\n"
                 f"Deploy Status - {status_label} ({status_state})\n"
                 f"Addon On - {addon_on}\n"
                 f"Display Name - {display_name}\n"
                 f"Email - {email}\n"
                 f"Password - {password}\n"
                 f"BOT_TOKEN - {bot_token}\n"
                 f"OP_COMMAND - {op_command}\n"
                 f"NONOP_COMMAND - {nonop_command}"
        )
        if status_state == "SUCCESS":
            return
        await asyncio.sleep(10)
    # Timeout reached
    addon_on = False
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=message_id,
        text=f"Name - {random_name}\n"
             f"Server Url - {server_url}\n"
             f"Deploy Status - Failed\n"
             f"Addon On - {addon_on}\n"
             f"Display Name - {display_name}\n"
             f"Email - {email}\n"
             f"Password - {password}\n"
             f"BOT_TOKEN - {bot_token}\n"
             f"OP_COMMAND - {op_command}\n"
             f"NONOP_COMMAND - {nonop_command}"
    )

# Global error handler
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Log the error for debugging
        print(f"Update {update} caused error {context.error}")
        # Notify the user
        if update and update.effective_message:
            await update.effective_message.reply_text(f"An error occurred: {str(context.error)}")
    except Exception as e:
        print(f"Error in error_handler: {str(e)}")

# /add command handler
async def add_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args = context.args
        if len(args) != 5 or args[1] != "-" or args[3] != "-":
            await update.message.reply_text("Usage: /add {email} - {password} - {display button name}")
            return
        email, password, display_name = args[0], args[2], args[4]
        accounts = load_accounts()
        if any(acc["display_name"] == display_name for acc in accounts if "display_name" in acc):
            await update.message.reply_text("Account with this display name already exists!")
            return
        if any(acc["email"] == email for acc in accounts):
            await update.message.reply_text("Account with this email already exists!")
            return
        accounts.append({"email": email, "password": password, "display_name": display_name})
        save_accounts(accounts)
        await update.message.reply_text(f"Account added: {display_name} ({email})")
    except Exception as e:
        await update.message.reply_text(f"Error adding account: {str(e)}")

# /remove command handler
async def remove_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args = context.args
        if len(args) != 1:
            await update.message.reply_text("Usage: /remove {display button name}")
            return
        display_name = args[0]
        accounts = load_accounts()
        initial_len = len(accounts)
        accounts = [acc for acc in accounts if acc.get("display_name") != display_name]
        if len(accounts) == initial_len:
            await update.message.reply_text(f"No account found with display name: {display_name}")
            return
        save_accounts(accounts)
        await update.message.reply_text(f"Account removed: {display_name}")
    except Exception as e:
        await update.message.reply_text(f"Error removing account: {str(e)}")

# /deploy command handler
async def deploy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        accounts = load_accounts()
        if not accounts:
            await update.message.reply_text("No accounts added. Use /add {email} - {password} - {display button name}")
            return
        keyboard = [
            [InlineKeyboardButton(acc.get("display_name", acc["email"]), callback_data=f"deploy_{i}")]
            for i, acc in enumerate(accounts)
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Select an account to deploy:", reply_markup=reply_markup)
        context.user_data["github_url"] = None
        context.user_data["awaiting_vars"] = False
    except Exception as e:
        await update.message.reply_text(f"Error in deploy command: {str(e)}")

# Callback query handler for account selection
async def select_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        account_index = int(query.data.split("_")[1])
        accounts = load_accounts()
        if account_index < 0 or account_index >= len(accounts):
            await query.message.reply_text("Invalid account selection.")
            return
        account = accounts[account_index]
        random_name = generate_random_name()
        github_url = "https://github.com/nahiefhspc/txesc"  # Default GitHub URL
        await query.message.reply_text(
            f"Name - {random_name}\nGithub Url - {github_url}\nPlease provide variables in the format:\nBOT_TOKEN=your_bot_token\nOP_COMMAND=your_op_command\nNONOP_COMMAND=your_nonop_command"
        )
        context.user_data["account"] = account
        context.user_data["random_name"] = random_name
        context.user_data["github_url"] = github_url
        context.user_data["awaiting_vars"] = True
    except Exception as e:
        await query.message.reply_text(f"Error selecting account: {str(e)}")

# Message handler for variables
async def handle_variables(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_vars", False):
        return
    try:
        text = update.message.text.strip()
        lines = text.split("\n")
        if len(lines) != 3:
            await update.message.reply_text("Please provide exactly three variables:\nBOT_TOKEN=your_bot_token\nOP_COMMAND=your_op_command\nNONOP_COMMAND=your_nonop_command")
            return
        bot_token = lines[0].split("=", 1)[1].strip() if lines[0].startswith("BOT_TOKEN=") else None
        op_command = lines[1].split("=", 1)[1].strip() if lines[1].startswith("OP_COMMAND=") else None
        nonop_command = lines[2].split("=", 1)[1].strip() if lines[2].startswith("NONOP_COMMAND=") else None
        if not all([bot_token, op_command, nonop_command]):
            await update.message.reply_text("Invalid format. Use:\nBOT_TOKEN=your_bot_token\nOP_COMMAND=your_op_command\nNONOP_COMMAND=your_nonop_command")
            return
        account = context.user_data["account"]
        random_name = context.user_data["random_name"]
        github_url = context.user_data["github_url"]
        display_name = account.get("display_name", account["email"])
        # Sign in
        id_token, owner_id = await sign_in(account["email"], account["password"])
        if not id_token:
            await update.message.reply_text(f"Sign-in failed: {owner_id}")
            return
        # Create server
        srv_id, server_url, error = await create_server(
            id_token, owner_id, github_url, bot_token, op_command, nonop_command, random_name
        )
        if error:
            await update.message.reply_text(error)
            return
        # Send initial deployment message
        message = await update.message.reply_text(
            f"Name - {random_name}\n"
            f"Server Url - {server_url}\n"
            f"Deploy Status - Starting...\n"
            f"Addon On - True\n"
            f"Display Name - {display_name}\n"
            f"Email - {account['email']}\n"
            f"Password - {account['password']}\n"
            f"BOT_TOKEN - {bot_token}\n"
            f"OP_COMMAND - {op_command}\n"
            f"NONOP_COMMAND - {nonop_command}"
        )
        # Poll status
        await poll_status(
            id_token,
            srv_id,
            update,
            context,
            message.message_id,
            random_name,
            server_url,
            account["email"],
            account["password"],
            display_name,
            bot_token,
            op_command,
            nonop_command
        )
        context.user_data["awaiting_vars"] = False
    except Exception as e:
        await update.message.reply_text(f"Error processing variables: {str(e)}")
        context.user_data["awaiting_vars"] = False

# Main function to run the bot
def main():
    # Replace with your bot token
    bot_token = os.environ.get("BOT_TOKEN", "8165963555:AAHlBnctxtvvjFz8ZUDWzTYeWco50AKgqJo")
    
    # Create a new event loop for the current thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Create the Application
        application = Application.builder().token(bot_token).build()
        
        # Add handlers
        application.add_handler(CommandHandler("add", add_account))
        application.add_handler(CommandHandler("remove", remove_account))
        application.add_handler(CommandHandler("deploy", deploy))
        application.add_handler(CallbackQueryHandler(select_account, pattern="^deploy_"))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_variables))
        application.add_error_handler(error_handler)
        
        # Initialize and start the application
        loop.run_until_complete(application.initialize())
        loop.run_until_complete(application.start())
        loop.run_until_complete(application.updater.start_polling())
        
        # Keep the loop running until interrupted
        loop.run_forever()
    except KeyboardInterrupt:
        print("Shutting down bot...")
    except Exception as e:
        print(f"Error running bot: {str(e)}")
    finally:
        # Properly shut down the application
        loop.run_until_complete(application.updater.stop())
        loop.run_until_complete(application.stop())
        loop.run_until_complete(application.shutdown())
        # Close MongoDB connection
        mongo_client.close()
        loop.close()

if __name__ == "__main__":
    main()

import os
import requests
import json
import re
from datetime import datetime, timedelta
import pytz
import uuid
import asyncio
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
import logging
from motor.motor_asyncio import AsyncIOMotorClient

# Set up logging
logging.basicConfig(filename='bot.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB settings
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb+srv://namanjain123eudhc:opmaster@cluster0.5iokvxo.mongodb.net/?retryWrites=true&w=majority")
DB_NAME = "TryBe"

# Asia/Kolkata timezone
KOLKATA_TZ = pytz.timezone('Asia/Kolkata')

# Telegram channel ID (replace with your channel ID, e.g., '-1001234567890')
CHANNEL_ID = '-1002173308278'

# Bot owner’s Telegram user ID (replace with your user ID, e.g., 123456789)
OWNER_ID = 7137002799

# Cooldown period (6 hours in seconds)
COOLDOWN_SECONDS = 6 * 3600  # 6 hours

# Message deletion delay for "Starting manual..." (2 minutes in seconds)
START_MESSAGE_DELETE_DELAY = 30

async def init_db():
    """Initialize MongoDB connection."""
    try:
        client = AsyncIOMotorClient(MONGODB_URI)
        db = client[DB_NAME]
        logging.info("MongoDB connected successfully")
        return db
    except Exception as e:
        logging.error(f"MongoDB connection error: {str(e)}")
        raise

async def get_start_message_id(db):
    """Get the stored message ID for the channel’s start message."""
    try:
        doc = await db.start_message_id.find_one()
        return doc.get('message_id') if doc else None
    except Exception as e:
        logging.error(f"Error getting start message ID: {str(e)}")
        return None

async def save_start_message_id(db, message_id):
    """Save the message ID for the channel’s start message."""
    try:
        await db.start_message_id.replace_one({}, {'message_id': message_id}, upsert=True)
    except Exception as e:
        logging.error(f"Error saving start message ID: {str(e)}")

async def get_restart_timestamps(db):
    """Get all restart timestamps."""
    try:
        timestamps = {}
        async for doc in db.restart_timestamps.find():
            timestamps[doc['showname']] = doc['timestamp']
        return timestamps
    except Exception as e:
        logging.error(f"Error getting restart timestamps: {str(e)}")
        return {}

async def save_restart_timestamp(db, showname):
    """Save the current timestamp for a showname after a restart."""
    try:
        await db.restart_timestamps.replace_one(
            {'showname': showname},
            {'showname': showname, 'timestamp': datetime.now(pytz.UTC).isoformat()},
            upsert=True
        )
    except Exception as e:
        logging.error(f"Error saving restart timestamp for {showname}: {str(e)}")

def get_remaining_cooldown(db, showname, timestamps):
    """Return remaining cooldown time in seconds for a showname, or None if no cooldown."""
    if showname not in timestamps:
        return None
    try:
        last_restart = datetime.fromisoformat(timestamps[showname]).replace(tzinfo=pytz.UTC)
        elapsed = (datetime.now(pytz.UTC) - last_restart).total_seconds()
        if elapsed < COOLDOWN_SECONDS:
            return COOLDOWN_SECONDS - elapsed
        return None
    except ValueError as e:
        logging.error(f"Error parsing timestamp for {showname}: {str(e)}")
        return None

def format_remaining_time(seconds):
    """Format seconds into a human-readable string (e.g., '4 hours, 23 minutes')."""
    if seconds <= 0:
        return "0 minutes"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    parts = []
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    return ", ".join(parts)

async def get_output_message_ids(db):
    """Get all output message IDs."""
    try:
        message_ids = {}
        async for doc in db.output_message_ids.find():
            message_ids[doc['showname']] = doc['message_id']
        return message_ids
    except Exception as e:
        logging.error(f"Error getting output message IDs: {str(e)}")
        return {}

async def save_output_message_id(db, showname, message_id):
    """Save the message ID for an account output message."""
    try:
        await db.output_message_ids.replace_one(
            {'showname': showname},
            {'showname': showname, 'message_id': message_id},
            upsert=True
        )
    except Exception as e:
        logging.error(f"Error saving output message ID for {showname}: {str(e)}")

async def get_accounts(db):
    """Get all Render accounts."""
    try:
        accounts = []
        async for doc in db.render_accounts.find():
            accounts.append({
                'showname': doc['showname'],
                'email': doc['email'],
                'password': doc['password']
            })
        return accounts
    except Exception as e:
        logging.error(f"Error getting accounts: {str(e)}")
        return []

async def save_account(db, showname, email, password):
    """Save a new Render account."""
    try:
        existing = await db.render_accounts.find_one({'showname': showname})
        if existing:
            return False, f"Account with showname '{showname}' already exists."
        await db.render_accounts.insert_one({
            'showname': showname,
            'email': email,
            'password': password
        })
        return True, f"Account '{showname}' added successfully."
    except Exception as e:
        logging.error(f"Error saving account: {str(e)}")
        return False, f"Failed to add account: {str(e)}"

async def remove_account(db, showname):
    """Remove a Render account by showname."""
    try:
        result = await db.render_accounts.delete_one({'showname': showname})
        if result.deleted_count == 0:
            return False, f"Account '{showname}' not found."
        await db.restart_timestamps.delete_one({'showname': showname})
        await db.output_message_ids.delete_one({'showname': showname})
        return True, f"Account '{showname}' removed successfully."
    except Exception as e:
        logging.error(f"Error removing account {showname}: {str(e)}")
        return False, f"Failed to remove account: {str(e)}"

def generate_request_id():
    """Generate a random UUID for render-request-id."""
    return str(uuid.uuid4())

def setup_session():
    """Set up a requests session with retry logic for rate limits."""
    session = requests.Session()
    retries = Retry(total=10, backoff_factor=1, status_forcelist=[429])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def sign_in(email, password, session):
    """Sign in to Render API and return id_token, expires_at, workspace_id, login_time."""
    url = "https://api.render.com/graphql"
    
    query = """
    mutation signIn($email: String!, $password: String!) {
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
    }
    """

    payload = {
        "operationName": "signIn",
        "variables": {
            "email": email,
            "password": password
        },
        "query": query
    }

    headers = {
        "accept": "*/*",
        "content-type": "application/json",
        "render-request-id": generate_request_id()
    }

    login_time = datetime.now(pytz.UTC).astimezone(KOLKATA_TZ)
    try:
        response = session.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if "data" in data and "signIn" in data["data"]:
            id_token = data["data"]["signIn"].get("idToken")
            expires_at = data["data"]["signIn"].get("expiresAt")
            workspaces = data["data"]["signIn"].get("workspaces", [])
            workspace_id = workspaces[0].get("id") if workspaces else None
            if id_token and workspace_id:
                return id_token, expires_at, workspace_id, login_time, None
        return None, None, None, login_time, "Sign-in failed: No idToken or workspace ID received"
    except requests.exceptions.RequestException as e:
        logging.error(f"Sign-in error: {str(e)}")
        return None, None, None, login_time, f"Error during sign-in: {str(e)}"

def get_services(owner_id, id_token, session):
    """Fetch services for the given owner_id."""
    url = "https://api.render.com/graphql"
    
    query = """
    query servicesForOwner($ownerId: String!, $includeSharedServices: Boolean, $emptyEnvironmentOnly: Boolean) {
      servicesForOwner(
        ownerId: $ownerId
        includeSharedServices: $includeSharedServices
        emptyEnvironmentOnly: $emptyEnvironmentOnly
      ) {
        id
        env {
          id
          name
          __typename
        }
        name
        userFacingType
        userFacingTypeSlug
        updatedAt
        region {
          id
          __typename
        }
        suspenders
        lastDeployedAt
        environment {
          id
          name
          project {
            id
            name
            owner {
              id
              __typename
            }
            __typename
          }
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
    }
    """

    payload = {
        "operationName": "servicesForOwner",
        "variables": {
            "ownerId": owner_id,
            "emptyEnvironmentOnly": True
        },
        "query": query
    }

    headers = {
        "accept": "*/*",
        "content-type": "application/json",
        "authorization": f"Bearer {id_token}",
        "render-request-id": generate_request_id()
    }

    try:
        response = session.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        services = data.get("data", {}).get("servicesForOwner", [])
        return services, None
    except requests.exceptions.RequestException as e:
        logging.error(f"Get services error: {str(e)}")
        return [], f"Error fetching services: {str(e)}"

def restart_render_server(server_id, api_key, session):
    """Restart a Render server by ID."""
    url = "https://api.render.com/graphql"
    
    query = """
    mutation restartServer($id: String!) {
      restartServer(id: $id) {
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
      permissions {
        accessShell {
          ...permissionResultFields
          __typename
        }
        deleteCustomDomain {
          ...permissionResultFields
          __typename
        }
        deleteServer {
          ...permissionResultFields
          __typename
        }
        deleteServerDisk {
          ...permissionResultFields
          __typename
        }
        manageStaticSiteConfiguration {
          ...permissionResultFields
          __typename
        }
        suspendServer {
          ...permissionResultFields
          __typename
        }
        updateMaintenanceMode {
          ...permissionResultFields
          __typename
        }
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
        limits {
          type
          limit
          __typename
        }
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
        manageEnvironmentSecrets {
          ...permissionResultFields
          __typename
        }
        moveEnvironmentResources {
          ...permissionResultFields
          __typename
        }
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
    }
    """
    payload = {
        "operationName": "restartServer",
        "variables": {
            "id": server_id
        },
        "query": query
    }
    headers = {
        "accept": "*/*",
        "content-type": "application/json",
        "authorization": f"Bearer {api_key}",
        "render-request-id": generate_request_id()
    }
    try:
        response = session.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data, None
    except requests.exceptions.HTTPError as e:
        try:
            error_response = e.response.json()
            error_message = f"Error restarting server {server_id}: {str(e)}\nError details: {json.dumps(error_response, indent=2)}"
        except ValueError:
            error_message = f"Error restarting server {server_id}: {str(e)}\nError response: {e.response.text}"
        logging.error(error_message)
        return None, error_message
    except requests.exceptions.RequestException as e:
        error_message = f"Error restarting server {server_id}: {str(e)}"
        logging.error(error_message)
        return None, error_message

async def delete_message(context: ContextTypes.DEFAULT_TYPE, chat_id, message_id):
    """Delete a message by chat_id and message_id."""
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logging.info(f"Deleted message {message_id} in chat {chat_id}")
    except Exception as e:
        logging.warning(f"Failed to delete message {message_id} in chat {chat_id}: {str(e)}")

async def schedule_start_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id, message_id):
    """Schedule deletion of the 'Starting manual...' message after 2 minutes."""
    context.job_queue.run_once(
        lambda ctx: asyncio.create_task(delete_message(ctx, chat_id, message_id)),
        START_MESSAGE_DELETE_DELAY,
        data={'chat_id': chat_id, 'message_id': message_id}
    )

async def delete_previous_output_message(context: ContextTypes.DEFAULT_TYPE, db, showname):
    """Delete the previous output message for a showname if it exists."""
    try:
        doc = await db.output_message_ids.find_one({'showname': showname})
        if doc:
            message_id = doc['message_id']
            await context.bot.delete_message(chat_id=CHANNEL_ID, message_id=message_id)
            logging.info(f"Deleted previous output message {message_id} for {showname}")
    except Exception as e:
        logging.warning(f"Failed to delete previous output message for {showname}: {str(e)}")

async def process_account(email, password, showname):
    """Process a single Render account and return formatted messages."""
    messages = []
    session = setup_session()
    logging.info(f"Processing account: {showname} ({email})")
    # Step 1: Sign in to get idToken, workspace_id, and login_time
    id_token, expires_at, workspace_id, login_time, error = sign_in(email, password, session)
    login_time_str = login_time.strftime('%Y-%m-%d %H:%M:%S %Z')
    now = datetime.now(pytz.UTC).astimezone(KOLKATA_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
    if error:
        messages.append(
            f"<blockquote><b>🌟 𝗕𝗢𝗧 - {showname}</b></blockquote>\n"
            f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
            f"<b>🔥 Restarted Successful</b> ➣ 🔴\n\n"
            f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
            f"<b>💝 𝐋𝐚𝐬𝐭 𝐑𝐞𝐬𝐭𝐚𝐫𝐭𝐞𝐝</b> ⪼ {now}\n\n"
            f"<b>------------------------------------------------</b>\n"
            f"<b>🤩 𝗪𝗘𝗕𝗦𝗜𝗧𝗘𝗦 ⊱ <a href='https://yashyasag.github.io/hiddens_officials'>𝐂𝐋𝐈𝐂𝐊 𝐇𝐄𝐑𝐄 🥰</a></b>"              
        )
        return messages
    if id_token and workspace_id:
        # Normalize the timestamp to handle variable digit microseconds
        match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})\d*Z", expires_at)
        if not match:
            messages.append(
                f"<blockquote><b>🌟 𝗕𝗢𝗧 - {showname}</b></blockquote>\n"
                f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                f"<b>🔥 Restarted Successful</b> ➣ 🔴\n\n"
                f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                f"<b>💝 𝐋𝐚𝐬𝐭 𝐑𝐞𝐬𝐭𝐚𝐫𝐭𝐞𝐝</b> ⪼ {now}\n\n"
                f"<b>------------------------------------------------</b>\n"
                f"<b>🤩 𝗪𝗘𝗕𝗦𝗜𝗧𝗘𝗦 ⊱ <a href='https://yashyasag.github.io/hiddens_officials'>𝐂𝐋𝐈𝐂𝐊 𝐇𝐄𝐑𝐄 🥰</a></b>"
            )
            return messages
        normalized_timestamp = match.group(1)
        try:
            expires_at_dt = datetime.strptime(normalized_timestamp, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=pytz.UTC)
            if expires_at_dt > datetime.now(pytz.UTC):
                # Step 2: Fetch services using workspace_id as ownerId
                services, error = get_services(workspace_id, id_token, session)
                if error or not services:
                    error_msg = error or "No services found"
                    messages.append(
                        f"<blockquote><b>🌟 𝗕𝗢𝗧 - {showname}</b></blockquote>\n"
                        f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                        f"<b>🔥 Restarted Successful</b> ➣ 🔴\n\n"
                        f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                        f"<b>💝 𝐋𝐚𝐬𝐭 𝐑𝐞𝐬𝐭𝐚𝐫𝐭𝐞𝐝</b> ⪼ {now}\n\n"
                        f"<b>------------------------------------------------</b>\n"
                        f"<b>🤩 𝗪𝗘𝗕𝗦𝗜𝗧𝗘𝗦 ⊱ <a href='https://yashyasag.github.io/hiddens_officials'>𝐂𝐋𝐈𝐂𝐊 𝐇𝐄𝐑𝐄 🥰</a></b>"
                    )
                    return messages
                # Step 3: Process each service
                for service in services:
                    service_id = service.get("id")
                    name = service.get("name", "Unknown")
                    suspenders = service.get("suspenders", [])
                    now = datetime.now(pytz.UTC).astimezone(KOLKATA_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
                    if suspenders:
                        messages.append(
                            f"<blockquote><b>🌟 𝗕𝗢𝗧 - {showname}</b></blockquote>\n"
                            f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                            f"<b>🔥 Restarted Successful</b> ➣ 🔴\n\n"
                            f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                            f"<b>💝 𝐋𝐚𝐬𝐭 𝐑𝐞𝐬𝐭𝐚𝐫𝐭𝐞𝐝</b> ⪼ {now}\n\n"
                            f"<b>------------------------------------------------</b>\n"
                            f"<b>🤩 𝗪𝗘𝗕𝗦𝗜𝗧𝗘𝗦 ⊱ <a href='https://yashyasag.github.io/hiddens_officials'>𝐂𝐋𝐈𝐂𝐊 𝐇𝐄𝐑𝐄 🥰</a></b>"
                            f"<b>Problem</b> - Service is suspended ({suspenders})"
                        )
                        continue
                    # Step 4: Restart the service
                    result, error = restart_render_server(service_id, id_token, session)
                    now = datetime.now(pytz.UTC).astimezone(KOLKATA_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
                    if error or result is None:
                        error_msg = error or "No response from server"
                        messages.append(
                            f"<blockquote><b>🌟 𝗕𝗢𝗧 - {showname}</b></blockquote>\n"
                            f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                            f"<b>🔥 Restarted Successful</b> ➣ 🔴\n\n"
                            f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                            f"<b>💝 𝐋𝐚𝐬𝐭 𝐑𝐞𝐬𝐭𝐚𝐫𝐭𝐞𝐝</b> ⪼ {now}\n\n"
                            f"<b>------------------------------------------------</b>\n"
                            f"<b>🤩 𝗪𝗘𝗕𝗦𝗜𝗧𝗘𝗦 ⊱ <a href='https://yashyasag.github.io/hiddens_officials'>𝐂𝐋𝐈𝐂𝐊 𝐇𝐄𝐑𝐄 🥰</a></b>"
                            f"<b>Problem</b> - {error_msg}"
                        )
                        continue
                    service_success = "data" in result and "restartServer" in result["data"]
                    messages.append(
                        f"<blockquote><b>🌟 𝗕𝗢𝗧 - {showname}</b></blockquote>\n"
                        f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                        f"<b>🔥 Restarted Successful</b> ➣ {'🟢' if service_success else '🔴'}\n"
                        f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                        f"<b>💝 𝐋𝐚𝐬𝐭 𝐑𝐞𝐬𝐭𝐚𝐫𝐭𝐞𝐝</b> ⪼ {now}\n\n"
                        f"<b>------------------------------------------------</b>\n"
                        f"<b>🤩 𝗪𝗘𝗕𝗦𝗜𝗧𝗘𝗦 ⊱ <a href='https://yashyasag.github.io/hiddens_officials'>𝐂𝐋𝐈𝐂𝐊 𝐇𝐄𝐑𝐄 🥰</a></b>"                        
                    )
            else:
                messages.append(
                    f"<blockquote><b>🌟 𝗕𝗢𝗧 - {showname}</b></blockquote>\n"
                    f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                    f"<b>🔥 Restarted Successful</b> ➣ 🔴\n\n"
                    f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                    f"<b>💝 𝐋𝐚𝐬𝐭 𝐑𝐞𝐬𝐭𝐚𝐫𝐭𝐞𝐝</b> ⪼ {now}\n\n"
                    f"<b>------------------------------------------------</b>\n"
                    f"<b>🤩 𝗪𝗘𝗕𝗦𝗜𝗧𝗘𝗦 ⊱ <a href='https://yashyasag.github.io/hiddens_officials'>𝐂𝐋𝐈𝐂𝐊 𝐇𝐄𝐑𝐄 🥰</a></b>"
                    f"<b>Problem</b> - Token has expired"
                )
        except ValueError as e:
            messages.append(
                f"<blockquote><b>🌟 𝗕𝗢𝗧 - {showname}</b></blockquote>\n"
                f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                f"<b>🔥 Restarted Successful</b> ➣ 🔴\n\n"
                f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
                f"<b>💝 𝐋𝐚𝐬𝐭 𝐑𝐞𝐬𝐭𝐚𝐫𝐭𝐞𝐝</b> ⪼ {now}\n\n"
                f"<b>------------------------------------------------</b>\n"
                f"<b>🤩 𝗪𝗘𝗕𝗦𝗜𝗧𝗘𝗦 ⊱ <a href='https://yashyasag.github.io/hiddens_officials'>𝐂𝐋𝐈𝐂𝐊 𝐇𝐄𝐑𝐄 🥰</a></b>"
                f"<b>Problem</b> - Error parsing timestamp: {str(e)}"
            )
    else:
        messages.append(
            f"<blockquote><b>🌟 𝗕𝗢𝗧 - {showname}</b></blockquote>\n"
            f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
            f"<b>🔥 Restarted Successful</b> ➣ 🔴\n\n"
            f"<b>━━━━━━━━━━━━━━━━━𖨠</b>\n\n"
            f"<b>💝 𝐋𝐚𝐬𝐭 𝐑𝐞𝐬𝐭𝐚𝐫𝐭𝐞𝐝</b> ⪼ {now}\n\n"
            f"<b>------------------------------------------------</b>\n"
            f"<b>🤩 𝗪𝗘𝗕𝗦𝗜𝗧𝗘𝗦 ⊱ <a href='https://yashyasag.github.io/hiddens_officials'>𝐂𝐋𝐈𝐂𝐊 𝐇𝐄𝐑𝐄 🥰</a></b>"
            f"<b>Problem</b> - Sorry Not Working 🥺"
        )
    return messages

async def create_keyboard(db):
    """Create inline keyboard with buttons for each Showname."""
    accounts = await get_accounts(db)
    keyboard = [
        [InlineKeyboardButton(account['showname'], callback_data=f"restart_{account['showname']}")]
        for account in accounts
    ]
    return InlineKeyboardMarkup(keyboard) if keyboard else None

async def send_or_update_start_message(context: ContextTypes.DEFAULT_TYPE, db):
    """Send or update the start message in the channel with account buttons."""
    keyboard = await create_keyboard(db)
    text = "<b>𝐇𝐞𝐥𝐥𝐨 𝐌𝐲 𝐃𝐞𝐚𝐫 𝐅𝐫𝐢𝐞𝐧𝐝 🥰</b>\n\n" \
           "<blockquote><b>Now you can Restart Any bot which Stops working 🥲\n" \
           "Please First Check Bot Stops or Not 🙏</b></blockquote>\n\n" \
           "<b>𝐎𝐔𝐑 𝐖𝐄𝐁𝐒𝐈𝐓𝐄 > <a href='https://yashyasag.github.io/hiddens_officials'>𝗖𝗟𝗜𝗖𝗞 𝗛𝗘𝗥𝗘 🕊</a></b>\n\n" \
           "<b>━━━━━━━━━━━━━━━━━━━━━𖨠</b>\n" \
           "<b>Credit Goes to <a href='https://t.me/HACKHEISTBOT'>𝗛𝗔𝗖𝗞𝗛𝗘𝗜𝗦𝗧 😈</a></b>\n" \
           "<b>━━━━━━━━━━━━━━━━━━━━━𖨠</b>"
    message_id = await get_start_message_id(db)
    
    try:
        if message_id:
            await context.bot.edit_message_text(
                chat_id=CHANNEL_ID,
                message_id=message_id,
                text=text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            logging.info(f"Updated start message in channel {CHANNEL_ID}, message_id: {message_id}")
        else:
            message = await context.bot.send_message(
                chat_id=CHANNEL_ID,
                text=text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            await save_start_message_id(db, message.message_id)
            logging.info(f"Sent new start message to channel {CHANNEL_ID}, message_id: {message.message_id}")
    except Exception as e:
        logging.error(f"Error sending/updating start message: {str(e)}")
        if message_id:
            try:
                message = await context.bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
                await save_start_message_id(db, message.message_id)
                logging.info(f"Sent new start message after failed edit, message_id: {message.message_id}")
            except Exception as e2:
                logging.error(f"Failed to send new start message: {str(e2)}")

async def add_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /add command to add a new Render account (owner only)."""
    db = context.bot_data['db']
    try:
        user_id = update.message.from_user.id
        if user_id != OWNER_ID:
            await update.message.reply_text("Only the bot owner can add accounts.")
            logging.warning(f"Unauthorized /add attempt by user {user_id}")
            return
        
        command = update.message.text.strip()
        if not command.startswith('/add '):
            await update.message.reply_text("Usage: /add {Showname} - {email} - {password}")
            return
        parts = command[5:].split(' - ')
        if len(parts) != 3:
            await update.message.reply_text("Invalid format. Use: /add {Showname} - {email} - {password}")
            return
        showname, email, password = [part.strip() for part in parts]
        if not showname or not email or not password:
            await update.message.reply_text("All fields are required: Showname, email, and password")
            return
        success, message = await save_account(db, showname, email, password)
        await update.message.reply_text(message)
        if success:
            await send_or_update_start_message(context, db)
    except Exception as e:
        logging.error(f"Error in /add command: {str(e)}")
        await update.message.reply_text(f"Error adding account: {str(e)}")

async def remove_account_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /remove command to remove a Render account (owner only)."""
    db = context.bot_data['db']
    try:
        user_id = update.message.from_user.id
        if user_id != OWNER_ID:
            await update.message.reply_text("Only the bot owner can remove accounts.")
            logging.warning(f"Unauthorized /remove attempt by user {user_id}")
            return
        
        command = update.message.text.strip()
        if not command.startswith('/remove '):
            await update.message.reply_text("Usage: /remove {Showname}")
            return
        showname = command[8:].strip()
        if not showname:
            await update.message.reply_text("Please provide a Showname.")
            return
        success, message = await remove_account(db, showname)
        await update.message.reply_text(message)
        if success:
            await send_or_update_start_message(context, db)
    except Exception as e:
        logging.error(f"Error in /remove command: {str(e)}")
        await update.message.reply_text(f"Error removing account: {str(e)}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command to show restart buttons."""
    db = context.bot_data['db']
    keyboard = await create_keyboard(db)
    if keyboard:
        await update.message.reply_text(
            "<b>𝐇𝐞𝐥𝐥𝐨 𝐌𝐲 𝐃𝐞𝐚𝐫 𝐅𝐫𝐢𝐞𝐧𝐝 🥰</b>\n\n" \
            "<blockquote><b>Now you can Restart Any bot which Stops working 🥲\n" \
            "Please First Check Bot Stops or Not 🙏</b></blockquote>\n\n" \
            "<b>𝐎𝐔𝐑 𝐖𝐄𝐁𝐒𝐈𝐓𝐄 > <a href='https://yashyasag.github.io/hiddens_officials'>𝗖𝗟𝗜𝗖𝗞 𝗛𝗘𝗥𝗘 🕊</a></b>\n\n" \
            "<b>━━━━━━━━━━━━━━━━━━━━━𖨠</b>\n" \
            "<b>Credit Goes to <a href='https://t.me/HACKHEISTBOT'>𝗛𝗔𝗖𝗞𝗛𝗘𝗜𝗦𝗧 😈</a></b>\n" \
            "<b>━━━━━━━━━━━━━━━━━━━━━𖨠</b>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text("No Render accounts added. Use /add {Showname} - {email} - {password} to add an account (owner only).")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button clicks for manual restarts with 6-hour cooldown."""
    db = context.bot_data['db']
    query = update.callback_query
    try:
        if query.data.startswith("restart_"):
            showname = query.data[len("restart_"):]
            accounts = await get_accounts(db)
            account = next((acc for acc in accounts if acc['showname'] == showname), None)
            if not account:
                await query.message.reply_text(f"Account '{showname}' not found.")
                await query.answer()
                return
            
            # Check cooldown
            timestamps = await get_restart_timestamps(db)
            remaining_seconds = get_remaining_cooldown(db, showname, timestamps)
            if remaining_seconds is not None:
                remaining_time = format_remaining_time(remaining_seconds)
                popup_message = f"Hey Buddy 🥰, you can't restart {showname} now. Please wait {remaining_time} to Restart 😊."
                logging.info(f"Attempting popup for {showname}, message: {popup_message}")
                try:
                    await query.answer(popup_message, show_alert=True)
                except Exception as e:
                    logging.error(f"Failed to show popup for {showname}: {str(e)}")
                    await query.answer("Cooldown active. Please try later.")  # Fallback
                return
            
            # Delete previous output message for this showname
            await delete_previous_output_message(context, db, showname)
            
            # Send "Starting manual..." message with HTML and schedule deletion
            start_message = await query.message.reply_text(
                f"<b><a href='https://yashyasag.github.io/hiddens_officials'>Starting doing restart for {showname}...</a></b>",
                parse_mode="HTML"
            )
            await schedule_start_message_deletion(context, CHANNEL_ID, start_message.message_id)
            
            # Proceed with restart
            messages = await process_account(account['email'], account['password'], showname)
            for message in messages:
                if len(message) > 4096:  # Telegram message limit
                    for i in range(0, len(message), 4096):
                        output_message = await query.message.reply_text(
                            message[i:i + 4096],
                            parse_mode="HTML"
                        )
                        await save_output_message_id(db, showname, output_message.message_id)
                else:
                    output_message = await query.message.reply_text(
                        message,
                        parse_mode="HTML"
                    )
                    await save_output_message_id(db, showname, output_message.message_id)
            
            # Update timestamp
            await save_restart_timestamp(db, showname)
            logging.info(f"Restart attempted for {showname}, timestamp updated")
        
        await query.answer()  # Ensure query is answered
    except Exception as e:
        error_msg = f"Error processing restart: {str(e)}"
        logging.error(error_msg)
        await query.message.reply_text(error_msg)
        await query.answer()

async def telegram_main():
    """Main function to start the Telegram bot."""
    BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "7167580859:AAGLqchS0nxQu2TjeZiG_u-sZhey4KuOl7Q")
    if not BOT_TOKEN:
        logging.error("TELEGRAM_BOT_TOKEN not set")
        print("Error: TELEGRAM_BOT_TOKEN not set")
        return
    
    # Initialize MongoDB
    try:
        db = await init_db()
    except Exception as e:
        print(f"Failed to initialize MongoDB: {str(e)}")
        return
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Store db in bot_data for access in handlers
    application.bot_data['db'] = db
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add", add_account))
    application.add_handler(CommandHandler("remove", remove_account_command))
    application.add_handler(CallbackQueryHandler(button_callback))
    
    logging.info("Starting Telegram bot...")
    print("Starting Telegram bot...")
    try:
        await application.initialize()
        await application.start()
        await send_or_update_start_message(application, db)
        await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        await asyncio.Event().wait()  # Keep bot running
    except Exception as e:
        logging.error(f"Bot error: {str(e)}")
        print(f"Bot error: {str(e)}")
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(telegram_main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
        print("Bot stopped by user")
    except Exception as e:
        logging.error(f"Error running bot: {str(e)}")
        print(f"Error running bot: {str(e)}")
    finally:
        tasks = [task for task in asyncio.all_tasks(loop) if task is not asyncio.current_task(loop)]
        for task in tasks:
            task.cancel()
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

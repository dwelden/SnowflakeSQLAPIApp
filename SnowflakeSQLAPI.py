# https://docs.snowflake.com/en/developer-guide/sql-api/guide.html
# https://api.developers.snowflake.com/

import json
import requests
import uuid

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session
from time import sleep

class SnowflakeSQLAPI:
    def __init__(self, account, token, tokenType, userAgent):

        self.APIUrl = f"https://{account}.snowflakecomputing.com/api/statements"
        self.headers = {
            'Authorization'                        : f"Bearer {token}",
            'Accept'                               : 'application/json',
            'Content-Type'                         : 'application/json',
            'User-Agent'                           : userAgent,
            'X-Snowflake-Authorization-Token-Type' : tokenType
        }

    def submit(self, database, schema, warehouse, role, request):
        """ Submit SQL statement(s) for execution. """

        # Generate UUID for API request id
        requestId = str(uuid.uuid4())

        # Build query parameters for URL
        params = {
            "requestId" : requestId,
            "async"     : request.get("async"),
            "pageSize"  : request.get("pageSize"),
            "nullable"  : request.get("nullable")
        }

        # Build request data payload
        payload = {
            "statement"         : ";".join(request['statements']),
            "resultSetMetaData" : {
                "format" : "json"
            },
            "database"          : database,
            "schema"            : schema,
            "warehouse"         : warehouse,
            "role"              : role
        }

        # Add optional payload entries if passed
        for key in ("timeout", "bindings", "parameters"):
            if key in request:
                payload[key] = f"{request[key]}"

        # Execute request, return response
        response = requests.post(
            self.APIUrl,
            params=params,
            headers=self.headers,
            data=payload
        )

        resultsets = []

        # Handle the response
        done = False
        while not done:
            if response.status_code == '200':
                # The statement was executed successfully.
                message, resultsets = self.success(requestId, response)
                done = True
            elif response.status_code == '202':
                # The execution of the statement is still in progress.
                done, message, response = self.inProgress(requestId, response)
            else:
                # Error handling for failed statement execution
                message = self.errorHandling(response)
                done = True

        return message, resultsets

    def success(self, requestId, response):
        """ Return resultsets """

        message = response.get("message")
        resultsets = []

        # Check if multiple statements executed
        statementHandles = response.get("statementHandles")
        if not statementHandles:
            statementHandle = response.get("statementHandle")
            statementHandles = []
            statementHandles.append(statementHandle)

        # Get result set for each statement handle
        resultsets = []
        for statementHandle in statementHandles:
            response = self.status(statementHandle, requestId)
            resultset = self.getResultset(statementHandle, response)
            resultsets.append(resultset)

        return message, resultsets

    def inProgress(self, requestId, response):
        """ Poll for statment completion """

        # Get first or only statement handle
        statementHandle = response.get("statementHandles")[0]
        if not statementHandle:
            statementHandle = response.get("statementHandle")

        # Keep checking for results while execution in progress
        # Abort after 5 minutes
        attempts = 0
        while (response.status_code == '202') and (attempts < 60):
            sleep(5)
            response = self.status(statementHandle, requestId)
            attempts += 1
        
        # If loop completed while still in progress, cancel request
        if response.status_code == '202':
            done = True
            message = self.cancel(statementHandle, requestId)
        else:
            done = False
            message = response.get("message")

        return done, message, response

    def getResultset(self, statementHandle, response):
        """ Get resultset from statement execution """

        data = response.get("data")

        payload = {}
        page = 1
        numPages = response["resultSetMetaData"]["numPages"]

        # Get data one page at a time until complete
        while page < numPages:
            # Get URL from link header for next page of results
            links = response.links
            url = links["next"]["url"]
            response = requests.get(
                url,
                headers=self.headers,
                data=payload
            )
            data.append(response.get("data"))
            page += 1

        resultset = {
            response[statementHandle] : data
        }

        return resultset

    def status(self, statementHandle, requestId, page=0, pageSize=10):
        """ Checks the status of the execution of the statement
            with the specified statement handle. If the statement
            was executed successfully, the operation returns the
            result set. """

        # Build query parameters for URL
        params = {
            "requestId" : requestId,
            "page"      : f"{page}",
            "pageSize"  : f"{pageSize}"
        }

        payload = {}

        url = f"{self.APIUrl}/{statementHandle}"
        response = requests.get(
            url,
            params=params,
            headers=self.headers,
            data=payload
        )
        return response

    def cancel(self, statementHandle, requestId):
        """ Cancels the execution of the statement with the
            specified statement handle. """

        # Build query parameters for URL
        params = {
            "requestId" : requestId
        }

        payload = {}

        url = f"{self.APIUrl}/{statementHandle}/cancel"
        response = requests.post(
            url,
            params=params,
            headers=self.headers,
            data=payload
        )

        # Return cancel message if success, otherwise error message
        if response.status_code == '200':
            message = response["CancelStatus"]["message"]
        else:
            message = self.errorHandling(response)

        return message

    def errorHandling(self, response):
        """ Return error message """

        # Determine status object from status code
        if response.status_code == '408':
            statusObject = response.get("QueryStatus")
        elif response.status_code == '422':
            statusObject = response.get("QueryFailureStatus")

        # If status object not found, default to response object
        if not statusObject:
            statusObject = response
        
        # Return status object message, or Unknown if message not found
        message = statusObject.get("message", "Unknown")
        return message

def getTokens(config):
    """ Get tokens using Resource Owner Password Credentials grant type """
    # https://docs.snowflake.com/en/user-guide/oauth-external.html

    # Initialize OAuth parameters from passed in configuration file
    keyvaultURI = config['Azure']['keyvaultURI']
    tokenUrl = config['Auth']['tokenUrl']
    role = config['Snowflake']['role']
    scope = [f"session:role:{role}"]
    
    # Retrieve secure variables from keyvault
    # https://docs.microsoft.com/en-us/azure/key-vault/secrets/quick-create-python
    credential = DefaultAzureCredential()
    keyvault = SecretClient(vault_url=keyvaultURI, credential=credential)
    clientId     = keyvault.get_secret("OAUTH_CLIENT_ID")
    clientSecret = keyvault.get_secret("OAUTH_CLIENT_SECRET")
    username     = keyvault.get_secret("OAUTH_USERNAME")
    password     = keyvault.get_secret("OAUTH_PASSWORD")

    # Create OAuth client and session
    client=LegacyApplicationClient(client_id=clientId)
    oauth = OAuth2Session(client=client)

    # Get access token
    response = oauth.fetch_token(
        token_url=tokenUrl,
        username=username,
        password=password,
        client_id=clientId,
        client_secret=clientSecret,
        scope=scope
    )
    tokens = {
        "accessToken"  : response.get("access_token"),
        "refreshToken" : response.get("refresh_token")
    }
    # token = {
    #      'access_token': 'zzz...yyy',
    #      'token_type': 'Bearer',
    #      'expires_in': 14400,
    #      'refresh_token': '123...789'
    # }

    return tokens

def main():

    # Load configuration
    with open('SnowflakeSQLAPI.json') as configFile:
        config = json.load(configFile)

    # Get authentication tokens for Snowflake
    tokens = getTokens(config)

    # Initialize API object
    snowflake = SnowflakeSQLAPI(
        f"{config['Snowflake']['account']}",
        tokens["accessToken"],
        f"{config['Auth']['tokenType']}",
        f"{config['Snowflake']['userAgent']}"
    )

    # Build request with statement and execution parameters
    request = {
        "statements" : [
            "select * from T where c1=?",
        ],
        "bindings": {
            "1" : {
                "type" : "FIXED",
                "value" : "123"
            },
        },
        "async" : "true",
        "timeout" : 10,
        "pageSize" : 10
    }
    
    # Submit request for execution
    message, resultsets = snowflake.submit(
        f"{config['Snowflake']['database']}",
        f"{config['Snowflake']['schema']}",
        f"{config['Snowflake']['warehouse']}",
        f"{config['Snowflake']['role']}",
        request
    )

    if resultsets:
        for resultset in resultsets:
            print(resultset)

    print(message)

if __name__ == '__main__':
    main()
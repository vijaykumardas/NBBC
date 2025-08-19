from fyers_api import accessToken
from fyers_api import fyersModel
import webbrowser


"""
In order to get started with Fyers API we would like you to do the following things first.
1. Checkout our API docs :   https://myapi.fyers.in/docs/
2. Create an APP using our API dashboard :   https://myapi.fyers.in/dashboard/

Once you have created an APP you can start using the below SDK 
"""

#### Generate an authcode and then make a request to generate an accessToken (Login Flow)

"""
1. Input parameters
"""
redirect_uri= "https://google.com"  ## redircet_uri you entered while creating APP.
client_id = "QCT5HT02YR-100"        ## Client_id here refers to APP_ID of the created app
secret_key = "WTBPRRCNUR"           ## app_secret key which you got after creating the app 
grant_type = "authorization_code"   ## The grant_type always has to be "authorization_code"
response_type = "code"              ## The response_type always has to be "code"
state = "sample"                    ##  The state field here acts as a session manager. you will be sent with the state field after successfull generation of auth_code 


### Connect to the sessionModel object here with the required input parameters
appSession = accessToken.SessionModel(client_id = client_id, redirect_uri = redirect_uri,response_type=response_type,state=state,secret_key=secret_key,grant_type=grant_type)

### Make  a request to generate_authcode object this will return a login url which you need to open in your browser from where you can get the generated auth_code 
generateTokenUrl = appSession.generate_authcode()

"""There are two method to get the Login url if  you are not automating the login flow
1. Just by printing the variable name 
2. There is a library named as webbrowser which will then open the url for you without the hasel of copy pasting
both the methods are mentioned below"""
print((generateTokenUrl))  
webbrowser.open(generateTokenUrl,new=1)

"""
run the code firstly upto this after you generate the auth_code comment the above code and start executing the below code """
##########################################################################################################################

### After succesfull login the user can copy the generated auth_code over here and make the request to generate the accessToken 
auth_code = input("Paste the auth_code generated from the first request")
appSession.set_token(auth_code)
response = appSession.generate_token()

### There can be two cases over here you can successfully get the acccessToken over the request or you might get some error over here. so to avoid that have this in try except block
try: 
    access_token = response["access_token"]
except Exception as e:
    print(e,response)  ## This will help you in debugging then and there itself like what was the error and also you would be able to see the value you got in response variable. instead of getting key_error for unsuccessfull response.




## Once you have generated accessToken now we can call multiple trading related or data related apis after that in order to do so we need to first initialize the fyerModel object with all the requried params.
"""
fyerModel object takes following values as arguments
1. accessToken : this is the one which you received from above 
2. client_id : this is basically the app_id for the particular app you logged into
"""
fyers = fyersModel.FyersModel(token=access_token,is_async=False,client_id=client_id,log_path="/home/downloads/")


### After this point you can call the relevant apis and get started with

####################################################################################################################
"""
1. User Apis : This includes (Profile,Funds,Holdings)
"""

print(fyers.get_profile())  ## This will provide us with the user related data 

print(fyers.funds())        ## This will provide us with the funds the user has 

print(fyers.holdings())    ## This will provide the available holdings the user has 


########################################################################################################################

"""
2. Transaction Apis : This includes (Tradebook,Orderbook,Positions)
"""

print(fyers.tradebook())   ## This will provide all the trade related information 

print(fyers.orderbook())   ## This will provide the user with all the order realted information 

print(fyers.positions())   ## This will provide the user with all the positions the user has on his end 


"""
DATA APIS : This includes following Apis(History,Quotes,MarketDepth)
"""

## Historical Data 

data = {"symbol":"NSE:SBIN-EQ","resolution":"D","date_format":"0","range_from":"1622097600","range_to":"1622097685","cont_flag":"1"}

print(fyers.history(data))

## Quotes 

data = {"symbols":"NSE:SBIN-EQ"}
print(fyers.quotes(data))


## Market Depth 

data = {"symbol":"NSE:SBIN-EQ","ohlcv_flag":"1"}
print(fyers.depth(data))


import os
from boxsdk import JWTAuth, Client

# Authenticate with Box using JWT
auth = JWTAuth(
    client_id=os.getenv('BOX_CLIENT_ID'),
    client_secret=os.getenv('BOX_CLIENT_SECRET'),
    enterprise_id=os.getenv('BOX_ENTERPRISE_ID'),
    jwt_key_id=os.getenv('BOX_PUBLIC_KEY_ID'),
    rsa_private_key_data=os.getenv('BOX_JWT_PRIVATE_KEY'),
    rsa_private_key_passphrase=os.getenv('BOX_PASSPHRASE').encode()
)

client = Client(auth)

# Specify the folder ID on Box where you want to upload the file
#284903365364 = /Stocks and Investments/NSEBSEBhavCopy
folder_id = '284903365364'  # Use '0' for the root folder or specify your folder's ID

# File to upload
file_path = 'output.csv'  # Adjust based on your generated CSV file name

with open(file_path, 'rb') as file:
    uploaded_file = client.folder(folder_id).upload_stream(file, file_name='output.csv')

print(f'File "{uploaded_file.name}" uploaded to Box with ID {uploaded_file.id}')

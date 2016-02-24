# xlMongo
A small utility for editing mongo collection from google spreadsheet.
Always have a working copy of mongo in your local , switch back to previous commits easily with multi snapshots maintained :)
Setup is pretty simple , just need the following  :
  - google developer account [https://console.developers.google.com]
 
 That's pretty much it!
#### How To Do
- login to google developer account and choose your project (or create a new if not already created)
- click on enable and manage api
- choose the DRIVE API  from the right site to enable
- post enabling go to credentials
- configure OAuth Consent screen with the name for your product  
- select create credentials and chose Oauth Client Id from drop down
- select other on Application Type page, give a name and CREATE!
- download the generated OAuth 2.0 client IDs ,place to the project root and rename it to client_secret.json (for now)
This is a standerd google auth process which is needed for OAuth 2.0 system as OAuth 1.5 is closing down soon!


Now for running the scripts:
- Edit the confi.yaml file to fill in basic details (comments explains each fields)
- Install all the required dependencies from requirement.txt using pip
- Run the xlMongo.py script,which will fetch all the data from the mongodb and will populate the drive with spreadsheet for each collection (all sheets will go to a specific folder defined in the applicationname field of config.yaml file)
And Voila!

#### Virtualenv
kindly create virtualenv before using pip (its a good idea in general to use virtual in any other system)
> virtualenv xlmongo;source xlmongo/bin/activate 

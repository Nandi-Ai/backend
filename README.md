#Instalations:

This is the backend of the project.

1. Install python  ```brew install python ```  
    - After installation please check that ```python3``` and ```pip3``` are installed.
    
        -   ```python3 --version```
    
        -   ```pip3 -V```

2. Go to ```cd mainapp```
    -   Create new file and name it ```local_settings.py```
    -   Download the content from https://drive.google.com/open?id=1g-cmrDHILbiLbpAwNvYys6jmSzO3xASN
    -   Instead of 'XXXX' add valid credentials.
    
   
3. Go to project root directory and install project requirements.

      - ```cd lynx-be```
    
      - ```pip3 install -r requirements.txt```
    
4. Install docker in order to setup the database.
    
    -   ```brew install docker```
     
    Install DB of the project via docker:
    
    -   ```docker-compose -f docker-compose.services.yml up -d```
    
5. Run migrations

    - ```python3 manage.py migrate```
    
6. Run server

    - ```python3 manage.py runserver```   

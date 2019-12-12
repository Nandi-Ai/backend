# Installations:

This is the backend of the project.

## Development

1. Install python  
    - Windows
    
    - Linux
    
    - Mac
    
        ```brew install python ```
          
    - After installation please check that ```python3``` and ```pip3``` are installed.
    
        -   ```python3 --version```
    
        -   ```pip3 -V```
      
2. Install virtual env (like pipenv pyenv or Aanaconda)
    
    - Windows
    
    - Linux
    
    - Mac
        - ```brew install pyenv```
       
   Go back to project:
        
    - ```cd lynx-be```
    
    - ```pyenv install <python-version>```(recommended to use python version >= 3.6.7)
    
    - ```pyenv virtualenv 3.6.7 <your-name-to-virtual-env>```
    
    - ```pyenv local backend-3.6.7```
    
    - Update your bash/zsh profile
    
        -   ```eval "$(pyenv init -)"```
    
        -   ```eval "$(pyenv virtualenv-init -)"```


2. Go to ```cd mainapp```
    -   Create new file and name it ```local_settings.py```
    -   Download the content from https://drive.google.com/open?id=1g-cmrDHILbiLbpAwNvYys6jmSzO3xASN
    -   Instead of 'XXXX' add valid credentials.
    
   
3. Go to project root directory and install project requirements.

      - ```cd lynx-be```
    
      - ```pip3 install -r requirements.txt```
    
4. Install docker in order to setup the database.
    
   - Windows
    
    - Linux
    
    - Mac
    
        -   ```brew cask install docker```
     
    Install DB of the project via docker:
    
    -   ```docker-compose -f docker-compose.services.yml up -d```
    
5. Run migrations

    - ```python3 manage.py migrate```
    
6. Run server

    - ```python3 manage.py runserver```
    
7. Create a user in Django admin:

    ```python manage.py createsuperuser --email=<your_email>```
    
    It will request from your to create new password

    
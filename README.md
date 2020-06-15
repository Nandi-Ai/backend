# lynx-be

This is the backend of the project.

## Prerequisites

### Python
- Windows - Download and setup from https://www.python.org/downloads/
- Linux -
- Mac - `brew install python`

After installation please check that ```python3``` and ```pip3``` are installed.

```
  python3 --version
  pip3 -V
```

### Docker
- Windows
- Linux
- Mac - `brew cask install docker` / download [Docker Desktop for Mac](https://hub.docker.com/editions/community/docker-ce-desktop-mac/)

### virtual env (like pipenv pyenv or Aanaconda)
- Windows
- Linux
- Mac - `brew install pyenv pyenv-virtualenv`

Go back to project:

(recommended to use python version >= 3.7.5)

```
   cd lynx-be
   pyenv install <python-version>
   pyenv virtualenv 3.7.5 <your-name-to-virtual-env>
   pyenv local backend-3.7.5
```

   Update your bash/zsh profile

```
   eval "$(pyenv init -)"
   eval "$(pyenv virtualenv-init -)"
```

### Requirements.txt
Go to project root directory and install project requirements.

```
   pip install -r requirements.txt
```

## Development

### Install the git hook scripts
run pre-commit install to set up the git hook scripts
```
pre-commit install --hook-type pre-commit --hook-type pre-push
```

to remove it type:
```
pre-commit uninstall --hook-type pre-commit --hook-type pre-push
```

### Local Settings
1. Go to `cd mainapp`
2. Create new file and name it `local_settings.py`
3. Download the content from https://drive.google.com/open?id=1g-cmrDHILbiLbpAwNvYys6jmSzO3xASN
4. Instead of 'XXXX' add valid credentials.
5. DB and logging settings for local_settings.py
```
if 'test' not in sys.argv:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': 'lynx',
            'USER': 'postgres',
            'PASSWORD': 'lynx',
            'HOST': os.environ.get('DATABASE_HOST', None) or 'localhost'
        }
    }
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': 'INFO',
    },
}
```

### Postgres
Install DB of the project via docker
```
docker-compose -f docker-compose.services.yml up -d
```


Create a user in Django admin:

```
python manage.py createsuperuser --email=<your_email>
```

It will request from your to create new password

### Run server

```
python manage.py runserver
```

## Run migrations

To update the local database with recent migrations use:
```
python manage.py migrate
```

## Running tests

To run the tests type:
```
python manage.py test
```

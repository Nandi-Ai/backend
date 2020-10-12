#!/usr/bin/env bash


NAME="lynx-be"                                  # Name of the application
DJANGODIR=/home/lynx/lynx-be             # Django project directory
SOCKFILE=/home/lynx/gunicorn.sock  # we will communicte using this unix socket
USER=lynx                                        # the user to run as
GROUP=lynx                             # the group to run as
NUM_WORKERS=3                                     # how many worker processes should Gunicorn spawn
TIMEOUT=120                             # timeout for guni workers
DJANGO_SETTINGS_MODULE=mainapp.settings             # which settings file should Django use
DJANGO_WSGI_MODULE=mainapp.wsgi                     # WSGI module name

echo "Starting $NAME as `whoami`"

# Activate the virtual environment
cd $DJANGODIR
#source /home/ubuntu/.virtualenvs/lynx-be/bin/activate
export DJANGO_SETTINGS_MODULE=$DJANGO_SETTINGS_MODULE
export PYTHONPATH=$DJANGODIR:$PYTHONPATH

# Create the run directory if it doesn't exist
RUNDIR=$(dirname $SOCKFILE)q
test -d $RUNDIR || mkdir -p $RUNDIR

# Start your Django Unicorn
# Programs meant to be run under supervisor should not daemonize themselves (do not use --daemon)
#--bind=unix:$SOCKFILE
exec gunicorn ${DJANGO_WSGI_MODULE}:application \
  --name $NAME \
  --workers $NUM_WORKERS \
  --timeout $TIMEOUT \
  --user=$USER --group=$GROUP \
  --bind=0.0.0.0:80 \
  --log-level=debug \
  --access-logfile - \
  --error-logfile - \
  --capture-output

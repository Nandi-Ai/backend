#!/usr/bin/env bash


NAME="lynx-be"                                  # Name of the application
DJANGODIR=/root/lynx-be             # Django project directory
SOCKFILE=/root/gunicorn.sock  # we will communicte using this unix socket
USER=root                                        # the user to run as
GROUP=root                             # the group to run as
NUM_WORKERS=3                                     # how many worker processes should Gunicorn spawn
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
exec gunicorn ${DJANGO_WSGI_MODULE}:application \
  --name $NAME \
  --workers $NUM_WORKERS \
  --user=$USER --group=$GROUP \
  --bind=unix:$SOCKFILE \
  --log-level=debug \
  --log-file=-
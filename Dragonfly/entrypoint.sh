#!bin/bash

echo "---Running Migrations---"

python manage.py migrate --noinput

echo "---Starting Server---"

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
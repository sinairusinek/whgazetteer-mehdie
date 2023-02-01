# Project path

/home/macbookpro/whgazetteer-mehdie

# git pull

sudo git pull origin main

# Gunicorn restart

sudo systemctl restart gunicorn.service

# Celery restart

sudo supervisorctl restart celery

# Celery log

sudo supervisorctl tail -10000 celery stderr
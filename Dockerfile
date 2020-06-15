FROM python:3.7.6-buster

ENV USER_NAME lynx
ENV APP lynx-be
ENV APP_HOME /home/${USER_NAME}
ENV APP_DIR /home/${USER_NAME}/${APP}

RUN apt update && apt install -y zip unzip locate gcc python3-dev git curl gnupg jq

RUN curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -

RUN useradd -ms /bin/bash ${USER_NAME}

WORKDIR ${APP_DIR}

RUN pip3 install awscli --upgrade

COPY requirements.txt .

RUN pip3 install -r requirements.txt

ADD . .

EXPOSE 80

ENTRYPOINT ["/home/lynx/lynx-be/guni.sh"]

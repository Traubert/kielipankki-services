FROM kaldi-align:latest

WORKDIR /home/app

# RUN apt-get update && apt-get dist-upgrade -y;
# RUN apt-get install -y python3;
# RUN pip3 install --upgrade pip
RUN find /opt/kaldi -name "*.so*" | xargs strip; \
    find /opt/intel ! -name "*.so*" -a -type f -delete;

RUN pip3 install Flask gunicorn==20.1.0 redis pydub requests
COPY . .
RUN chmod -R a+rwx /home/app; chmod -R a+rwx /opt/kaldi/egs;

ENTRYPOINT []

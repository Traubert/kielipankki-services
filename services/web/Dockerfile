FROM debian:stable-slim as builder

WORKDIR /usr/src/hfst

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        make autoconf automake libtool pkgconf \
	g++ libglib2.0-dev swig bison flex gettext gawk \
	wget ca-certificates bzip2 python3-dev unzip git swig;

ENV LANG C.UTF-8
    
RUN set -eux; \
    wget https://github.com/hfst/hfst/releases/download/v3.15.3/hfst-3.15.3.tar.bz2; \
    tar -x --bzip2 -f hfst-3.15.3.tar.bz2; \
    rm -f hfst-3.15.3.tar.bz2; \
    cd hfst-3.15.3; \
    autoreconf -i; \
    ./configure --with-unicode-handler=glib \
        --without-sfst \
	--without-foma \
	--enable-no-tools \
	--enable-lookup \
	--enable-tokenize \
	--enable-pmatch \
	--enable-optimized-lookup \
	--disable-dependency-tracking; \
    make; \
    make install-strip; \
    cd python ; python3 setup.py build_ext --inplace; python3 setup.py install; \
    cd ../../; rm -rf hfst-3.15.3;

# COPY ./finnish-tagtools-1.6.0.zip ./

RUN set -eux; \
    wget https://korp.csc.fi/download/finnish-tagtools/v1.5/finnish-tagtools-1.5.1.zip; \
    unzip finnish-tagtools-1.5.1.zip; \
    rm -f finnish-tagtools-1.5.1.zip; \
    cd finnish-tagtools-1.5.1; \
    # for busybox
    sed -i 's/--target-directory=/-t /g' Makefile; \
    make install ; \
    cd ..; rm -rf finnish-tagtools-1.5.1;
    # unzip finnish-tagtools-1.6.0.zip; \
    # rm -f finnish-tagtools-1.6.0.zip; \
    # cd finnish-tagtools-1.6.0; \
    # # for busybox
    # sed -i 's/--target-directory=/-t /g' Makefile; \
    # make install ; \
    # cd ..; rm -rf finnish-tagtools-1.6.0;

WORKDIR /usr/src

RUN set -eux; \
    git clone https://github.com/Traubert/nlp-tools.git; \
    cd nlp-tools/word_embeddings/c++; \
    make;

FROM python:3.9-slim-bullseye
WORKDIR /usr/src/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install -y \
    libglib2.0-0

COPY --from=builder /usr/local/ /usr/local/
RUN ldconfig;

RUN set -eux; \
    pip3 install torch==1.7.1+cpu --no-cache-dir -f https://download.pytorch.org/whl/torch_stable.html;

# install dependencies
RUN pip install --upgrade pip
COPY ./requirements.txt /usr/src/app/requirements.txt
RUN pip install -r requirements.txt

# copy project
COPY . /usr/src/app/
COPY --from=builder /usr/src/nlp-tools/word_embeddings/c++/_embutils.so /usr/src/app/texttools/
COPY --from=builder /usr/src/nlp-tools/word_embeddings/c++/embutils.py /usr/src/app/texttools/



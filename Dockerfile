FROM python:3.7.1

RUN mkdir -p /device-failure-prediction

WORKDIR /device-failure-prediction

ADD . /device-failure-prediction

RUN make

CMD ["make", "run"]
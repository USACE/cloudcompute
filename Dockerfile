FROM golang:1.19-alpine3.15 AS dev

RUN apk add --no-cache \
    build-base \
    gcc \
    git
    
# Production container
#FROM golang:1.19-alpine3.15 AS prod
#RUN apk add --update docker openrc
#RUN rc-update add docker boot
#WORKDIR /app
#COPY --from=dev /app/main .
#CMD [ "./main" ]
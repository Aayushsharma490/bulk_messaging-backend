FROM ghcr.io/puppeteer/puppeteer:20

USER root

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm ci --only=production

COPY . .

EXPOSE 5000

CMD [ "node", "server.js" ]

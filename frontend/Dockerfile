FROM node:10

WORKDIR /usr/express
COPY package*.json ./
RUN npm install

COPY . .
EXPOSE 3000

CMD [ "node", "bin/www" ]

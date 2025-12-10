# Use an official Node.js runtime as the base image
FROM node:20

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
COPY package*.json ./
RUN npm install --only=production

# Bundle app source
COPY . .

# Cloud Run expects the service to listen on $PORT
ENV PORT=8080

# Start the app
CMD ["npm", "start"]
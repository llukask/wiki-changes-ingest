const config = require('config');

const PROJECT_ID = config.get("projectId");
const TOPIC_NAME = config.get("topicName");
const EVENT_STREAM_URI = config.get("eventStreamUri");

const EventSource = require("eventsource");
const PubSub = require('@google-cloud/pubsub');
const winston = require('winston');

const logger = new winston.Logger({
  transports: [
    new (winston.transports.Console)({
      timestamp: function() {
        return Date.now();
      },
      formatter: function(options) {
        // Return string will be passed to logger.
        return (new Date(options.timestamp()).toISOString()) +' '+ options.level.toUpperCase() +' '+ (options.message ? options.message : '') +
          (options.meta && Object.keys(options.meta).length ? '\n\t'+ JSON.stringify(options.meta) : '' );
      }
    })
  ]
});



const pubsubClient = PubSub({
  projectId: PROJECT_ID
});


const topic = pubsubClient.topic(TOPIC_NAME);
logger.info("Connected to topic " + TOPIC_NAME);

let changeStream = new EventSource(EVENT_STREAM_URI);
logger.info("Connected to EventStream " + EVENT_STREAM_URI);

let ids = [];

let cnt = 0;
changeStream.onmessage = msg => {
  // console.log(typeof msg.data);
  let data = JSON.parse(msg.data);
  if(data.type === 'edit') {
    //console.log(JSON.stringify(data));
    if(ids.includes(data.id)) {
      winston.info("Got duplicate " + data.id);
    }
    let formatted = {
      bot: data.bot,
      comment: data.comment,
      id: data.id,
      new_length: data.length.new,
      old_length: data.length.old,
      minor: data.minor,
      namespace: data.namespace,
      patrolled: data.patrolled,
      new_rev: data.revision.new,
      old_rev: data.revision.old,
      server_name: data.server_name,
      server_script_path: data.server_script_path,
      server_url: data.server_url,
      timestamp: data.timestamp,
      title: data.title,
      type: data.type,
      user: data.user,
      wiki: data.wiki
    };
    ids.push(formatted.id);
    topic.publish(formatted, (err, msgIds, apiResponse) => {
      cnt++;
      if(err) {
        logger.error(err);
      }
    });
  }
};

setInterval(() => {
  logger.info("Got " + cnt + " events in the last 10s!");
  cnt = 0;
}, 10000);

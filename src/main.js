const EventSource = require("eventsource");
const PubSub = require('@google-cloud/pubsub');

const projectId = "tweeting-164909";

const pubsubClient = PubSub({
  projectId: projectId
});

const topicName = 'wiki-changes';

const topic = pubsubClient.topic(topicName);
console.log(topic);
let changeStream = new EventSource(
  "https://stream.wikimedia.org/v2/stream/recentchange");
let cnt = 0;
changeStream.onmessage = msg => {
  // console.log(typeof msg.data);
  let data = JSON.parse(msg.data);
  if(data.type === 'edit') {
    //console.log(JSON.stringify(data));
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
    topic.publish(formatted, (err, msgIds, apiResponse) => {
      cnt++;
      if(err) {
        console.error(err);
      }
    });
  }
};

setInterval(() => {
  console.log("Got " + cnt + " in the last 10s!");
  cnt = 0;
}, 10000);

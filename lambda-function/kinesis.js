const AWS = require('aws-sdk');
const kinesisConstant = require('./kinesisConstants'); //Keep it consistent
const kinesis = new AWS.Kinesis({
  apiVersion: kinesisConstant.API_VERSION, //optional
  //accessKeyId: '<you-can-use-this-to-run-it-locally>', //optional
  //secretAccessKey: '<you-can-use-this-to-run-it-locally>', //optional
  region: kinesisConstant.REGION
});

const savePayload = (payload) => {
//We can only save strings into the streams
  if( typeof payload !== kinesisConstant.PAYLOAD_TYPE) {
    try {
      payload = JSON.stringify(payload);
    } catch (e) {
      console.log(e);
    }
  }

  let params = {
    Data: payload,
    PartitionKey: kinesisConstant.PARTITION_KEY,
    StreamName: kinesisConstant.STREAM_NAME
  };

  kinesis.putRecord(params, function(err, data) {
    if (err) console.log(err, err.stack);
    else     console.log('Record added:',data);
  });
};

exports.save = (payload) => {
  const params = {
    StreamName: kinesisConstant.STREAM_NAME,
  };

  kinesis.describeStream(params, function(err, data) {
    if (err) console.log(err, err.stack);
    else {
      //Make sure stream is able to take new writes (ACTIVE or UPDATING are good)
      if(data.StreamDescription.StreamStatus === kinesisConstant.STATE.ACTIVE
        || data.StreamDescription.StreamStatus === kinesisConstant.STATE.UPDATING ) {
        savePayload(payload);
      } else {
        console.log(`Kinesis stream ${kinesisConstant.STREAM_NAME} is ${data.StreamDescription.StreamStatus}.`);
        console.log(`Record Lost`, JSON.parse(payload));
      }
    }
  });
};
require("dotenv").config();

const fs = require("fs");
const { parse } = require("csv-parse");
const async = require("async");
const { v4: uuid } = require("uuid");
const AWS = require("aws-sdk");

// --- start user config ---

const CSV_FILENAME = "./data/data.csv";

const AWS_CREDENTIALS_PROFILE = process.env["AWS_CREDENTIALS_PROFILE"];
const DYNAMODB_REGION = process.env["DYNAMODB_REGION"];
const DYNAMODB_TABLENAME = process.env["DYNAMODB_TABLENAME"];

// --- end user config ---

const credentials = new AWS.SharedIniFileCredentials({
  profile: AWS_CREDENTIALS_PROFILE,
});
AWS.config.credentials = credentials;
const docClient = new AWS.DynamoDB.DocumentClient({
  region: DYNAMODB_REGION,
});

const rs = fs.createReadStream(CSV_FILENAME);
const parser = parse(
  {
    columns: true,
    delimiter: ",",
  },
  function (err, data) {
    var split_arrays = [],
      size = 25;

    while (data.length > 0) {
      split_arrays.push(data.splice(0, size));
    }
    data_imported = false;
    chunk_no = 1;

    async.each(
      split_arrays,
      function (item_data, callback) {
        const params = {
          RequestItems: {},
        };
        params.RequestItems[DYNAMODB_TABLENAME] = [];
        item_data.forEach((item) => {
          for (key of Object.keys(item)) {
            // An AttributeValue may not contain an empty string
            if (item[key] === "") delete item[key];
          }

          params.RequestItems[DYNAMODB_TABLENAME].push({
            PutRequest: {
              Item: {
                id: uuid(),
                ...item,
              },
            },
          });
        });

        docClient.batchWrite(params, function (err, res, cap) {
          console.log("done going next");
          if (err == null) {
            console.log("Success chunk #" + chunk_no);
            data_imported = true;
          } else {
            console.log(err);
            console.log("Fail chunk #" + chunk_no);
            data_imported = false;
          }
          chunk_no++;
          callback();
        });
      },
      function () {
        // run after loops
        console.log("all data imported....");
      }
    );
  }
);
rs.pipe(parser);

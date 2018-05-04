/* postgis2mapbox.js
Purpose: Query data from a PostGIS database and upload it to Mapbox as a vector tileset
Limitations: Uploads data as GeoJSON (not mbtiles) limited to 1GB.
*/

const { Client } = require('pg');
const dbgeo = require('dbgeo');
const MapboxSDK = require('mapbox');
const AWS = require('aws-sdk');
const argv = require('minimist')(process.argv.slice(2));

const mapboxClient = new MapboxSDK(argv.m); // -m
const connectionString = argv.p; // -p
const username = argv.u; // -u
const tilesetName = argv.o; // -o
const tmpFile = '/tmp/tmp.geojson'; // must have write access to a this directory!
const fields = !argv.f ? '*' : argv.f.split(', ');
const table = argv.t;
const where = !argv.w ? '1=1' : argv.w;

let mapboxCredentials;

//defults for PG are process.env variables (consider using those)
const pgClient = new Client({ connectionString: connectionString });

pgClient.connect((error, success) => {
  if (error) {
    console.log('There was a problem connecting to the database.', error);
  } else {
    console.log('postgres connection successful');
  }
});

pgClient.query(
  `SELECT ${fields} FROM ${table} WHERE ${where}`,
  (error, result) => {
    if (error) {
      console.log('There was a problem executing the query.', error);
    } else {
      console.log('parsing query results');
      pgClient.end();
      parseGeoJson(result);
    }
  }
);

function parseGeoJson(queryResult) {
  // convert the postgis query to geojson
  dbgeo.parse(
    queryResult.rows,
    {
      outputFormat: 'geojson'
    },
    (err, res) => {
      if (err) {
        console.log(
          'There was a problem converting the query result to geojson.',
          err
        );
      } else {
        console.log('GeoJSON parsed!');
        uploadToS3(res);
      }
    }
  );
}

function uploadToS3(geojson) {
  console.log('Getting Mapbox credentials.');
  // stage the file in an S3 bucket (provided by Mapbox)
  // get S3 credentials from Mapbox
  mapboxClient.createUploadCredentials((err, res) => {
    if (err) {
      console.log('There was a problem obtaining Mapbox credentials.', err);
    } else {
      mapboxCredentials = res;
      const s3 = new AWS.S3({
        accessKeyId: mapboxCredentials.accessKeyId,
        secretAccessKey: mapboxCredentials.secretAccessKey,
        sessionToken: mapboxCredentials.sessionToken,
        region: 'us-east-1'
      });

      // upload to S3 Bucket
      console.log('Staging file in AWS S3 bucket.');
      s3.putObject(
        {
          Bucket: mapboxCredentials.bucket,
          Key: mapboxCredentials.key,
          Body: JSON.stringify(geojson)
        },
        (err, res) => {
          if (err) {
            console.log('There was a problem uploading to the S3 bucket.', err);
          } else {
            uploadToMapbox();
          }
        }
      );
    }
  });
}

function uploadToMapbox() {
  // upload to Mapbox
  mapboxClient.createUpload(
    {
      name: tilesetName,
      tileset: [username, tilesetName].join('.'),
      url: mapboxCredentials.url
    },
    (err, upload) => {
      if (err) {
        console.log('There was a problem uploading the file to Mapbox.', err);
        process.exit(1);
      } else {
        console.log('Upload started. Check Mapbox.com for progress.', upload);
        process.exit(0);
      }
    }
  );
}

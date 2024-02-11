const { Storage } = require('@google-cloud/storage');
const csvParser = require('csv-parser');
const { BigQuery } = require('@google-cloud/bigquery');

const storage = new Storage();
const bucketName = 'gcf_weather_etl-41200-kevin-assignment';
const bucket = storage.bucket(bucketName);
const bq = new BigQuery();
const datasetId = 'gcf_weather_etl_dataset';
const tableId = 'station weather data';

exports.readObservation = (file, context) => {
    console.log(`  Event: ${context.eventId}`);
    console.log(`  Event Type: ${context.eventType}`);
    console.log(`  Bucket: ${file.bucket}`);
    console.log(`  File: ${file.name}`);
}

async function processFiles() {
  try {
    const [files] = await bucket.getFiles();

    for (const file of files) {
      const readStream = file.createReadStream();
      const parser = csvParser({ separator: ',' });

      readStream.pipe(parser)
        .on('data', (row) => {
          row['station'] = file.name;
          const modifiedRow = convertToNumeric(row);
          stationObject(modifiedRow); // Pass the modified row to stationObject
        })
        .on('end', () => {
          console.log(`CSV ${file.name} successfully processed.`);
        })
        .on('error', (err) => {
          console.error(`Error processing file ${file.name}:`, err);
        });
    }
  } catch (err) {
    console.error('Error retrieving files:', err);
  }
}

//define entrypoint
const stationObject = (weatherObject) => {
  //just pass the object as the input would be an object
  writeToBQ([weatherObject])
}

//create helper (must be async function)
async function writeToBQ(rows) {
  try {
    await bq
      .dataset(datasetId)
      .table(tableId)
      .insert(rows); // now inserts each incoming row and fills it to be an object
    rows.forEach((row) => {
      console.log(`Inserted row: ${JSON.stringify(row)}`);
    });
  } catch (err) {
    if (err.name === 'PartialFailureError') {
      // Handle partial failure
      err.errors.forEach((error) => {
        console.error(`Error inserting row: ${JSON.stringify(error.row)}`);
        console.error(`Reason: ${error.errors[0].message}`);
      });
    } else {
      console.error(`ERROR: ${err}`);
    }
  }
}

function logCSV(csvData){ // Logs every line of data
  csvData.forEach((row) => {
    for ( [key, value] of Object.entries(row)){
      console.log(`${key} : ${value}`)
    }
  });
}

function convertToNumeric (dictionary) { // Converts dictionary to numeric value
  const result = {}
  for (const key in dictionary) {
    if (Object.prototype.hasOwnProperty.call(dictionary, key)) {
      let value = dictionary[key]
      let numValue = parseFloat(value); // converts value into float
      if (numValue == -9999){ // turns super negatives into nulls
        numValue = null
      } else {
        switch (key) { // stat fixing
          case "airtemp": // converts airtemp to Fahrenheit
            numValue = (numValue * 9/5) + 32;
            break;
          case "pressure": // converts pressure to inches
            numValue = numValue / 33.8639;
            break;
        }
      }
      result[key] = numValue; // Actually does the converting from dictionary to a float
    }
  }
  return result;
}

// Calling Functions

processFiles().catch(console.error)

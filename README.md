# Amazo

Amazo is a modeling tool for Amazon's DynamoDB (inspired by [Mongoose](http://mongoosejs.com/))

## Getting Started

### Installation

    $ npm i amazo

### Example

Here's a simple example:

```js
const amazo = require("amazo");
const AWS = require("aws-sdk");
const dynamo = new AWS.DynamoDB.DocumentClient();

amazo.setDocumentClient(dynamo);

// Create cat model
const Cat = amazo.model({
  tableName: "cats-table",
  hashKey: "id",
  sortKey: "name",
  index: null
});

// Create a new cat object and save to DynamoDB
const garfield = await Cat.save({
  id: 666,
  name: "Garfield"
});

// Find in DynamoDB
let badcat = await Cat.findOne({ id: 666, name: "Garfield" });
console.log(`Never trust a smiling cat. - ${badCat.name}`);
```

## API Docs

The documentation can be found at https://amazo.nanthakumar.ca.

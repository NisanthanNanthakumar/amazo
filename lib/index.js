const AWS = require("aws-sdk");
const util = require("util");

function Amazo() {
  this.models = {};

  this.defaults = {
    create: true,
    waitForActive: true, // Wait for table to be created
    waitForActiveTimeout: 180000, // 3 minutes
    prefix: "", // prefix_Table
    suffix: "" // Table_suffix
  }; // defaults
}

Amazo.prototype.model = function(schema) {
  const model = {};
  model.getDatabaseTableName = () =>
    schema.tableName ? schema.tableName : null;
  model.getPartitionKey = () =>
    schema.partitionKey ? schema.partitionKey : null;
  model.getSortKey = () => (schema.sortKey ? schema.sortKey : null);
  model.getIndex = () => (schema.index ? schema.index : null);
  model.findOne = async (query, options = {}) => {
    try {
      const dynamoQuery = generateQuery(model, query);

      dynamoQuery.Limit = 1;

      console.log(this);
      let result = await this.query(dynamoQuery);

      return result.Items[0];
    } catch (error) {
      console.log({ error });
    }
  };

  model.find = async (query, options = {}) => {
    try {
      const dynamoQuery = generateQuery(model, query);
      let result = await this.query(dynamoQuery);
      return result.Items;
    } catch (error) {
      console.log({ error });
    }
  };

  model.scan = async (query, options = {}) => {};

  model.save = async (query, options = {}) => {};

  model.deletOne = async (query, options = {}) => {};

  model.updateOne = async (query, options = {}) => {};

  model.findOneAndDelete = async (query, options = {}) => {};

  model.findOneAndUpdate = async (query, options = {}) => {};

  model.updateOne = async (query, options = {}) => {};

  return model;
};

/**
 * Document client for executing nested scans
 */

Amazo.prototype.setDocumentClient = function(documentClient) {
  for (prop in documentClient) {
    if (typeof documentClient[prop] === "function") {
      this[prop] = util.promisify(documentClient[prop].bind(documentClient));
    }
  }
  console.log(this)
};

Amazo.prototype.Amazo = Amazo;

module.exports = new Amazo();

const generateQuery = (model, queryOptions) => {
  console.log({ model, queryOptions });
  const partitionKey = model.getPartitionKey();
  const sortKey = model.getSortKey();
  const expressionAttributeNames = {};
  const expressionAttributeValues = {};
  const comparisonOperators = queryOptions.comparisonOperators || {};

  expressionAttributeNames["#hkeyname"] = partitionKey;
  expressionAttributeValues[":hkeyvalue"] = queryOptions[partitionKey];
  const hashKeyComparisonOperator = comparisonOperators[partitionKey] || "=";
  let keyConditionExpression = null;

  if (queryOptions[sortKey]) {
    const rangeKeyComparisonOperator = comparisonOperators[sortKey] || "=";
    expressionAttributeNames["#rkeyname"] = sortKey;
    expressionAttributeValues[":rkeyvalue"] = queryOptions[sortKey];
    keyConditionExpression = `#hkeyname ${hashKeyComparisonOperator} :hkeyvalue and #rkeyname ${rangeKeyComparisonOperator} :rkeyvalue`;
  } else {
    keyConditionExpression = `#hkeyname ${hashKeyComparisonOperator} :hkeyvalue`;
  }
  const query = {
    TableName: model.getDatabaseTableName(),
    IndexName: model.getIndex(),
    ExpressionAttributeNames: expressionAttributeNames,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeValues: expressionAttributeValues
  };
  return query;
};

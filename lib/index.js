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
  model.getHashKey = () => (schema.hashKey ? schema.hashKey : null);
  model.getSortKey = () => (schema.sortKey ? schema.sortKey : null);
  model.getIndex = () => (schema.index ? schema.index : null);

  model.findOne = async (query, options = {}) => {
    try {
      const dynamoQuery = generateQuery(model, query, options);

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
      const dynamoQuery = generateQuery(model, query, options);
      console.log({ dynamoQuery });

      let result = await this.query(dynamoQuery);
      return result.Items;
    } catch (error) {
      console.log({ error });
    }
  };

  model.scan = async (query, options = {}) => {
    try {
      let dynamoQuery = generateScanQuery(model, query, options);
      console.log({ dynamoQuery });

      let result = await this.scan(dynamoQuery);

      return result.Items;
    } catch (error) {
      console.log({ error });
    }
  };

  model.save = async (query, options = {}) => {
    try {
      let dynamoQuery = generateSaveQuery(model, query, options);

      let result = await this.put(dynamoQuery);
      return result;
    } catch (error) {
      console.log({ error });
    }
  };
  //   model.deletOne = async (query, options = {}) => {};

  //   model.updateOne = async (query, options = {}) => {};

  //   model.findOneAndDelete = async (query, options = {}) => {};

  //   model.findOneAndUpdate = async (query, options = {}) => {};

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
};

Amazo.prototype.Amazo = Amazo;

module.exports = new Amazo();

const comparisonConversion = {
  eq: "=",
  lt: "<",
  lte: "<=",
  gt: ">",
  gte: ">="
};
const generateQuery = (model, queryObj, options) => {
  /**
   * options = {
   *  limit: Number,
   *  descending: Boolean // will default to false
   *  filter: {
   *    key: [ comparisonOperator, ..args],
   *    key: [ comparisonOperator, ..args]
   *  }
   * }
   */
  console.log({ model, queryObj, options });
  const hashKey = model.getHashKey();
  const sortKey = model.getSortKey();
  const expressionAttributeNames = {};
  const expressionAttributeValues = {};

  let filterExpression = "";
  let hashKeyComparisonOperator = null;
  let rangeKeyComparisonOperator = null;
  let keyConditionExpression = null;

  if (queryObj[hashKey]) {
    hashKeyComparisonOperator = "=";
    expressionAttributeNames["#hkeyname"] = hashKey;
    expressionAttributeValues[":hkeyvalue"] = queryObj[hashKey];
  }

  if (queryObj[sortKey]) {
    rangeKeyComparisonOperator = "=";
    expressionAttributeNames["#rkeyname"] = sortKey;
    expressionAttributeValues[":rkeyvalue"] = queryObj[sortKey];
  }

  if (options.filter) {
    Object.keys(options.filter).forEach(item => {
      const [operator, ...args] = options.filter[item];
      const comparisonOperator = comparisonConversion[operator];
      console.log({ comparisonOperator, item, args, sortKey, hashKey });
      if (item === hashKey) {
        hashKeyComparisonOperator = comparisonOperator;
        expressionAttributeNames["#hkeyname"] = hashKey;
        expressionAttributeValues[":hkeyvalue"] = args[0];
      }
      if (item === sortKey) {
        rangeKeyComparisonOperator = comparisonOperator;
        expressionAttributeNames["#rkeyname"] = sortKey;
        expressionAttributeValues[":rkeyvalue"] = args[0];
      }
      if (item !== hashKey && item !== sortKey) {
        expressionAttributeNames["#" + item] = item;
        expressionAttributeValues[":" + item] = args[0];
        if (filterExpression.length > 0) {
          filterExpression += "and ";
        }
        filterExpression += "#" + item + comparisonOperator + ":" + item + " ";
      }
    });
  }

  if (hashKeyComparisonOperator) {
    keyConditionExpression = `#hkeyname ${hashKeyComparisonOperator} :hkeyvalue`;
  }
  if (rangeKeyComparisonOperator) {
    keyConditionExpression = `#rkeyname ${rangeKeyComparisonOperator} :rkeyvalue`
  }
  if (hashKeyComparisonOperator && rangeKeyComparisonOperator) {
    keyConditionExpression = `#hkeyname ${hashKeyComparisonOperator} :hkeyvalue and #rkeyname ${rangeKeyComparisonOperator} :rkeyvalue`;
  } 
  
  const query = {
    TableName: model.getDatabaseTableName(),
    IndexName: model.getIndex(),
    ExpressionAttributeNames: expressionAttributeNames,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeValues: expressionAttributeValues
  };

  if (filterExpression && filterExpression.length > 0) {
    query.FilterExpression = filterExpression;
  }

  if (options.limit && options.limit > 0) {
    query.Limit = options.limit;
  }

  if (options.descending) {
    query.ScanIndexForward = false;
  }

  return query;
};

const generateScanQuery = (model, queryOptions) => {
  const expressionAttributeNames = {};
  const expressionAttributeValues = {};
  const comparisonOperators = queryOptions.comparisonOperators || {};

  let filterExpression = "";
  if (queryOptions) {
    Object.keys(queryOptions).map(item => {
      expressionAttributeNames["#" + item] = item;
      expressionAttributeValues[":" + item] = queryOptions[item];
      if (filterExpression.length > 0) {
        filterExpression += "and ";
      }
      const comparisonOperator = comparisonOperators[item] || "=";
      filterExpression += "#" + item + comparisonOperator + ":" + item + " ";
    });
  }

  const query = {
    TableName: model.getDatabaseTableName(),
    IndexName: model.getIndex()
  };
  if (filterExpression && filterExpression.length > 0) {
    query.FilterExpression = filterExpression;
    query.ExpressionAttributeNames = expressionAttributeNames;
    query.ExpressionAttributeValues = expressionAttributeValues;
  }

  if (queryOptions.limit && queryOptions.limit > 0) {
    query.Limit = queryOptions.limit;
  }

  return query;
};

const generateSaveQuery = (model, item) => {
  const newItem = Object.assign({}, data);
  const hashKey = model.getHashKey();
  const rangeKey = model.getRangeKey();

  if (!newItem[hashKey]) {
    newItem[hashKey] = shortid.generate();
  }

  if (rangeKey && !newItem[rangeKey]) {
    newItem[rangeKey] = shortid.generate();
  }

  const query = {
    TableName: model.getDatabaseTableName(),
    Item: newItem
  };

  return query;
};

const AWS = require("aws-sdk");
const util = require("util");

function Amazo() {}

Amazo.prototype.model = function(schema) {
  const model = {};
  model.getDatabaseTableName = () =>
    schema.tableName ? schema.tableName : null;
  model.getHashKey = () => (schema.hashKey ? schema.hashKey : null);
  model.getSortKey = () => (schema.sortKey ? schema.sortKey : null);
  model.getIndex = () => (schema.index ? schema.index : null);

  model.findOne = async (query = {}, options = { filter: {} }) => {
    try {
      const dynamoQuery = generateFindQuery(model, query, options);

      dynamoQuery.Limit = 1;

      // console.log(this);
      let result = await this.query(dynamoQuery);

      return result.Items[0];
    } catch (error) {
      console.log({ error });
    }
  };

  model.find = async (query = {}, options = { filter: {} }) => {
    try {
      const dynamoQuery = generateFindQuery(model, query, options);
      console.log({ dynamoQuery });

      let result = await this.query(dynamoQuery);

      return result.Items;
    } catch (error) {
      console.log({ error });
    }
  };

  model.scan = async (query = {}, options = { filter: {} }) => {
    try {
      let dynamoQuery = generateScanQuery(model, query, options);
      console.log({ dynamoQuery });

      let result = await this.scan(dynamoQuery);

      return result.Items;
    } catch (error) {
      console.log({ error });
    }
  };

  model.save = async (data = {}, options = {}) => {
    try {
      let dynamoQuery = generateSaveQuery(model, data, options);

      let result = await this.put(dynamoQuery);
      return result;
    } catch (error) {
      console.log({ error });
    }
  };
  //   model.deletOne = async (query, options = {}) => {};

  model.findOneAndUpdate = async (query = {}, data = {}, options = {}) => {
    try {
      let dynamoQuery = generateUpdateQuery(model, query, data, options);

      let firstItem = await model.findOne(query);
      if (!firstItem) {
        throw new errors.QueryError("Can't update non-existing item");
      } else {
        let result = await this.update(dynamoQuery);
        return result;
      }
    } catch (error) {
      console.log({ error });
    }
  };

  model.findOneAndDelete = async (query = {}, options = { filter: {} }) => {
    try {
      let dynamoQuery = generateDeleteQuery(model, query, options);
      console.log({ dynamoQuery });

      let result = await this.delete(dynamoQuery);

      return result;
    } catch (error) {
      console.log({ error });
    }
  };

  return model;
};

/**
 * Document client for executing nested scans
 */

Amazo.prototype.setDocumentClient = function(documentClient) {
  for (prop in documentClient) {
    // console.log(prop);
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

const sortConversion = {
  ASC: false,
  DESC: true
};
const generateFindQuery = (model, queryObj, options) => {
  /**
   * options = {
   *  limit: Number,
   *  sort: "ASC" || "DESC" // will default to ASC
   *  filter: {
   *    key: [ comparisonOperator, ..args],
   *    key: [ comparisonOperator, ..args]
   *  }
   * }
   */
  // console.log({ model, queryObj, options });
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
      // console.log({ comparisonOperator, item, args, sortKey, hashKey });
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
    keyConditionExpression = `#rkeyname ${rangeKeyComparisonOperator} :rkeyvalue`;
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

  if (options.sort) {
    query.ScanIndexForward = sortConversion[options.sort];
  }

  return query;
};

const generateScanQuery = (model, queryObj, options) => {
  /**
   * options = {
   *  limit: Number,
   *  filter: {
   *    key: [ comparisonOperator, ..args],
   *    key: [ comparisonOperator, ..args]
   *  }
   * }
   */

  const expressionAttributeNames = {};
  const expressionAttributeValues = {};

  let filterExpression = "";

  let newQueryObj = {};

  Object.keys(queryObj).forEach(key => {
    newQueryObj[key] = ["eq", queryObj[key]];
  });

  let newObj = Object.assign({}, newQueryObj, options.filter);

  if (newObj) {
    Object.keys(newObj).forEach(item => {
      const [operator, ...args] = newObj[item];
      const comparisonOperator = comparisonConversion[operator] || "=";
      console.log({ comparisonOperator, item, args });
      expressionAttributeNames["#" + item] = item;
      expressionAttributeValues[":" + item] = args[0];
      if (filterExpression.length > 0) {
        filterExpression += "and ";
      }
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

  if (options.limit && options.limit > 0) {
    query.Limit = options.limit;
  }

  return query;
};

const generateSaveQuery = (model, data, options) => {
  const newItem = Object.assign({}, data);

  const hashKey = model.getHashKey();
  const rangeKey = model.getSortKey();

  if (!newItem[hashKey]) {
    throw new errors.ModelError("Item could not be saved. Item needs hash key");
  }

  if (rangeKey && !newItem[rangeKey]) {
    throw new errors.ModelError("Item could not be saved. Item needs range key.");
  }

  const query = {
    TableName: model.getDatabaseTableName(),
    Item: newItem
  };

  return query;
};

const generateUpdateQuery = (model, queryObj, data, options) => {
  const hashKey = model.getHashKey();
  const rangeKey = model.getSortKey();

  const keyCondition = {};
  keyCondition[hashKey] = queryObj[hashKey];

  if (rangeKey) {
    keyCondition[rangeKey] = queryObj[rangeKey];
  }

  const newItem = Object.assign({}, data);

  delete newItem[hashKey];

  if (rangeKey) {
    delete newItem[rangeKey];
  }

  const updates = {};
  for (const key in newItem) {
    if (newItem.hasOwnProperty(key)) {
      updates[key] = {
        Action: "PUT",
        Value: newItem[key]
      };
    }
  }

  const updateQuery = {
    TableName: model.getDatabaseTableName(),
    Key: keyCondition,
    AttributeUpdates: updates
  };

  return updateQuery;
};

const generateDeleteQuery = (model, queryObj, options) => {
  const expressionAttributeNames = {};
  const expressionAttributeValues = {};
  const keyAttributeValues = {};

  let conditionExpression = "";

  let newQueryObj = {};

  Object.keys(queryObj).forEach(key => {
    newQueryObj[key] = ["eq", queryObj[key]];
    keyAttributeValues[key] = queryObj[key];
  });

  let newObj = Object.assign({}, newQueryObj, options.filter);

  if (newObj) {
    Object.keys(newObj).forEach(item => {
      const [operator, ...args] = newObj[item];
      const comparisonOperator = comparisonConversion[operator] || "=";
      // console.log({ comparisonOperator, item, args });
      expressionAttributeNames["#" + item] = item;
      expressionAttributeValues[":" + item] = args[0];
      if (conditionExpression.length > 0) {
        conditionExpression += "and ";
      }
      conditionExpression += "#" + item + comparisonOperator + ":" + item + " ";
    });
  }

  const query = {
    TableName: model.getDatabaseTableName(),
    IndexName: model.getIndex()
  };

  if (conditionExpression && conditionExpression.length > 0) {
    query.ConditionExpression = conditionExpression;
    query.ExpressionAttributeNames = expressionAttributeNames;
    query.ExpressionAttributeValues = expressionAttributeValues;
    query.Key = keyAttributeValues;
  }

  return query;
};


function ErrorType (defaultMessage, errorName) {
  class newError extends Error {
    constructor(message) {
      super();
      this.name = errorName
      this.message =  message || defaultMessage
    }
  }

  return newError;
}

const errors = {
  'ModelError': new ErrorType('Error with model', 'ModelError'),
  'QueryError': new ErrorType('Error with query', 'QueryError'),
  'ScanError': new ErrorType('Error with scan', 'ScanError')
};
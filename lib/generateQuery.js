const errors = require("./errors")
const {comparisonConversion, sortConversion} = require("./constants")

const generateFindQuery = (model, queryObj, options) => {
  /**
   * options = {
   *  filter: { }
   * }
   */

  const hashKey = model.getHashKey();
  const sortKey = model.getSortKey();
  const expressionAttributeNames = {};
  const expressionAttributeValues = {};

  let filterExpression = "";
  let hashKeyComparisonOperator = null;
  let sortKeyComparisonOperator = null;
  let keyConditionExpression = null;

  if (queryObj[hashKey]) {
    hashKeyComparisonOperator = "=";
    expressionAttributeNames["#hkeyname"] = hashKey;
    expressionAttributeValues[":hkeyvalue"] = queryObj[hashKey];
  }

  if (queryObj[sortKey]) {
    sortKeyComparisonOperator = "=";
    expressionAttributeNames["#rkeyname"] = sortKey;
    expressionAttributeValues[":rkeyvalue"] = queryObj[sortKey];
  }

  if (options.filters) {
    for (let item in options.filters) {
      const { name, values, comparison } = options.filters[item];
      const comparisonOperator = comparisonConversion[comparison];
      if (name === hashKey) {
        hashKeyComparisonOperator = comparisonOperator;
        expressionAttributeNames["#hkeyname"] = hashKey;
        expressionAttributeValues[":hkeyvalue"] = values[0];
      }
      if (name === sortKey) {
        sortKeyComparisonOperator = comparisonOperator;
        expressionAttributeNames["#rkeyname"] = sortKey;
        expressionAttributeValues[":rkeyvalue"] = values[0];
      }
      if (name !== hashKey && name !== sortKey) {
        expressionAttributeNames["#" + name] = name;
        expressionAttributeValues[":" + name] = values[0];
        if (filterExpression.length > 0) {
          filterExpression += "and ";
        }
        filterExpression += "#" + name + comparisonOperator + ":" + name + " ";
      }
    }
  }

  if (hashKeyComparisonOperator) {
    keyConditionExpression = `#hkeyname ${hashKeyComparisonOperator} :hkeyvalue`;
  }
  if (sortKeyComparisonOperator) {
    keyConditionExpression = `#rkeyname ${sortKeyComparisonOperator} :rkeyvalue`;
  }
  if (hashKeyComparisonOperator && sortKeyComparisonOperator) {
    keyConditionExpression = `#hkeyname ${hashKeyComparisonOperator} :hkeyvalue and #rkeyname ${sortKeyComparisonOperator} :rkeyvalue`;
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
    newQueryObj[key] = {
      name: key,
      values: [queryObj[key]],
      comparison: "EQ"
    };
  });

  let newObj = Object.assign({}, newQueryObj, options.filters);

  if (newObj) {
    Object.keys(newObj).forEach(item => {
      const { name, values, comparison } = newObj[item];
      const comparisonOperator = comparisonConversion[comparison] || "=";
      expressionAttributeNames["#" + name] = name;
      expressionAttributeValues[":" + name] = values[0];
      if (filterExpression.length > 0) {
        filterExpression += "and ";
      }
      filterExpression += "#" + name + comparisonOperator + ":" + name + " ";
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
  const sortKey = model.getSortKey();

  if (!newItem[hashKey]) {
    throw new errors.ModelError("Item could not be saved. Item needs hash key");
  }

  if (sortKey && !newItem[sortKey]) {
    throw new errors.ModelError(
      "Item could not be saved. Item needs range key."
    );
  }

  const query = {
    TableName: model.getDatabaseTableName(),
    Item: newItem
  };

  return query;
};

const generateUpdateQuery = (model, queryObj, data) => {
  const hashKey = model.getHashKey();
  const sortKey = model.getSortKey();

  const keyCondition = {};
  keyCondition[hashKey] = queryObj[hashKey];

  if (sortKey && queryObj[sortKey]) {
    keyCondition[sortKey] = queryObj[sortKey];
  }

  const newItem = Object.assign({}, data);

  delete newItem[hashKey];

  if (sortKey) {
    delete newItem[sortKey];
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

const generateDeleteQuery = (model, queryObj) => {
  const expressionAttributeNames = {};
  const expressionAttributeValues = {};
  const keyAttributeValues = {};

  let conditionExpression = "";

  //   Object.keys(queryObj).forEach(key => {
  //     newQueryObj[key] = ["eq", queryObj[key]];
  //     keyAttributeValues[key] = queryObj[key];
  //   });

  Object.keys(queryObj).forEach(key => {
    keyAttributeValues[key] = queryObj[key];
    expressionAttributeNames["#" + key] = key;
    expressionAttributeValues[":" + key] = queryObj[key];
    if (conditionExpression.length > 0) {
      conditionExpression += "and ";
    }
    conditionExpression += "#" + key + "=" + ":" + key + " ";
  });

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

module.exports = {
  generateFindQuery,
  generateScanQuery,
  generateSaveQuery,
  generateUpdateQuery,
  generateDeleteQuery
};

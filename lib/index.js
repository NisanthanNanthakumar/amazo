const AWS = require("aws-sdk");
const util = require("util");

function Amazo() {
  this.dynamodb = {};
}

Amazo.prototype.model = function(schema) {
  const model = {};
  model.getDatabaseTableName = () =>
    schema.tableName ? schema.tableName : null;
  model.getHashKey = () => (schema.hashKey ? schema.hashKey : null);
  model.getSortKey = () => (schema.sortKey ? schema.sortKey : null);
  model.getIndex = () => (schema.index ? schema.index : null);

  model.findOne = (query = {}, options = { filters: {} }) => {
    return new Query(
      model,
      query,
      options,
      (modelQuery = "findOne"),
      this.dynamoDocumentClient
    );
  };

  model.find = (query = {}, options = { filters: {} }) => {
    return new Query(
      model,
      query,
      options,
      (modelQuery = "find"),
      this.dynamoDocumentClient
    );
  };

  model.scan = (query = {}, options = { filters: {} }) => {
    return new Query(
      model,
      query,
      options,
      (modelQuery = "scan"),
      this.dynamoDocumentClient
    );
  };

  model.save = async (data = {}, options = {}) => {
    try {
      let dynamoQuery = generateSaveQuery(model, data, options);

      let result = await this.dynamodb.put(dynamoQuery);
      return result;
    } catch (error) {
      throw new error.QueryError(error.message);
    }
  };

  model.findOneAndUpdate = (
    query = {},
    data = {},
    options = { filters: {} }
  ) => {
    return new Query(
      model,
      query,
      options,
      (modelQuery = "findOneAndUpdate"),
      this.dynamoDocumentClient,
      data
    );
  };

  model.findOneAndDelete = (query = {}, options = { filters: {} }) => {
    return new Query(
      model,
      query,
      options,
      (modelQuery = "findOneAndDelete"),
      this.dynamoDocumentClient
    );
  };

  return model;
};

/**
 * Document client for executing nested scans
 */

Amazo.prototype.setDocumentClient = function(documentClient) {
  this.dynamoDocumentClient = documentClient;
  for (prop in documentClient) {
    if (typeof documentClient[prop] === "function") {
      this.dynamodb[prop] = util.promisify(
        documentClient[prop].bind(documentClient)
      );
    }
  }
};

Amazo.prototype.Amazo = Amazo;

module.exports = new Amazo();

const comparisonConversion = {
  EQ: "=",
  LT: "<",
  LTE: "<=",
  GT: ">",
  GTE: ">="
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

function ErrorType(defaultMessage, errorName) {
  class newError extends Error {
    constructor(message) {
      super();
      this.name = errorName;
      this.message = message || defaultMessage;
    }
  }

  return newError;
}

const errors = {
  ModelError: new ErrorType("Error with model", "ModelError"),
  QueryError: new ErrorType("Error with query", "QueryError"),
  ScanError: new ErrorType("Error with scan", "ScanError")
};

function Query(model, query, options, modelQuery, documentClient, updateData) {
  this.model = model;
  this.options = options || { filters: {} };
  this.query = query;
  this.modelQuery = modelQuery;
  this.updateData = updateData;
  this.dynamodb = {};
  this.buildState = false;
  this.validationError = null;

  for (prop in documentClient) {
    if (typeof documentClient[prop] === "function") {
      this.dynamodb[prop] = util.promisify(
        documentClient[prop].bind(documentClient)
      );
    }
  }
}

Query.prototype.filter = function(filter) {
  if (this.validationError) {
    return this;
  }
  if (this.buildState) {
    this.validationError = new errors.QueryError(
      "Invalid Query state: filter() must follow comparison"
    );
    return this;
  }
  if (typeof filter === "string") {
    this.buildState = "filter";
    this.currentFilter = filter;
    if (this.options.filters[filter]) {
      this.validationError = new errors.QueryError(
        "Invalid Query state: %s filter can only be used once",
        filter
      );
      return this;
    }
    this.options.filters[filter] = { name: filter };
  }

  return this;
};

const VALID_OPERATORS = ["EQ", "LTE", "LT", "GE", "GTE"];

Query.prototype.comparisonOperator = function(vals, comp) {
  if (this.validationError) {
    return this;
  }
  if (this.buildState === "hashKey") {
    if (comp !== "EQ") {
      this.validationError = new errors.QueryError(
        "Invalid Query state: eq must follow query()"
      );
      return this;
    }
    this.query.hashKey.value = vals[0];
  } else if (this.buildState === "sortKey") {
    if (VALID_OPERATORS.indexOf(comp) < 0) {
      this.validationError = new errors.QueryError(
        `Invalid Query state: ${comp} must follow filter()`
      );
      return this;
    }
    this.query.sortKey.values = vals;
    this.query.sortKey.comparison = comp;
  } else if (this.buildState === "filter") {
    this.options.filters[this.currentFilter].values = vals;
    this.options.filters[this.currentFilter].comparison = comp;
  } else {
    this.validationError = new errors.QueryError(
      `Invalid Query state: ${comp} must follow query(), where() or filter()`
    );
    return this;
  }

  this.buildState = false;
  this.notState = false;

  return this;
};

Query.prototype.eq = function(val) {
  return this.comparisonOperator([val], "EQ");
};

Query.prototype.lt = function(val) {
  return this.comparisonOperator([val], "LT");
};

Query.prototype.lte = function(val) {
  return this.comparisonOperator([val], "LTE");
};

Query.prototype.gte = function(val) {
  return this.comparisonOperator([val], "GTE");
};

Query.prototype.gt = function(val) {
  return this.comparisonOperator([val], "GT");
};

Query.prototype.exec = async function() {
  try {
    let { modelQuery } = this;

    let dynamoQuery;
    let result;
    if (modelQuery === "findOne") {
      let { model, query, options } = this;

      dynamoQuery = generateFindQuery(model, query, options);
      dynamoQuery.Limit = 1;
      result = await this.dynamodb.query(dynamoQuery);
      return result.Items;
    }
    if (modelQuery === "find") {
      let { model, query, options } = this;

      dynamoQuery = generateFindQuery(model, query, options);
      result = await this.dynamodb.query(dynamoQuery);
      return result.Items;
    }
    if (modelQuery === "scan") {
      let { model, query, options } = this;

      dynamoQuery = generateScanQuery(model, query, options);
      result = await this.dynamodb.scan(dynamoQuery);
      return result.Items;
    }

    if (modelQuery === "findOneAndUpdate") {
      let { model, query, updateData, options } = this;
      dynamoFindOneQuery = generateFindQuery(model, query, options);
      dynamoFindOneQuery.Limit = 1;

      let resultItem = await this.dynamodb.query(dynamoFindOneQuery);
      if (!resultItem.Items[0]) {
        throw new errors.QueryError("Can't update non-existing item");
      } else {
        let updateQueryObj = {};
        let item = resultItem.Items[0];

        let hashKey = model.getHashKey();
        let sortKey = model.getSortKey();

        if (hashKey && item[hashKey]) {
          updateQueryObj[hashKey] = item[hashKey];
        }
        if (sortKey && item[sortKey]) {
          updateQueryObj[sortKey] = item[sortKey];
        }
        let dynamoUpdateQuery = generateUpdateQuery(
          model,
          updateQueryObj,
          updateData
        );
        let result = await this.dynamodb.update(dynamoUpdateQuery);
        return result;
      }
    }

    if (modelQuery === "findOneAndDelete") {
      let { model, query, options } = this;

      dynamoFindOneQuery = generateFindQuery(model, query, options);
      dynamoFindOneQuery.Limit = 1;

      let resultItem = await this.dynamodb.query(dynamoFindOneQuery);
      if (!resultItem.Items[0]) {
        throw new errors.QueryError("Can't update non-existing item");
      } else {
        let deleteQueryObj = {};
        let item = resultItem.Items[0];

        let hashKey = model.getHashKey();
        let sortKey = model.getSortKey();

        if (hashKey && item[hashKey]) {
          deleteQueryObj[hashKey] = item[hashKey];
        }
        if (sortKey && item[sortKey]) {
          deleteQueryObj[sortKey] = item[sortKey];
        }
        let dynamoQuery = generateDeleteQuery(model, deleteQueryObj);
        let result = await this.dynamodb.delete(dynamoQuery);

        return result;
      }
    }

    return result;
  } catch (error) {
    throw new errors.QueryError(error.message);
  }
};

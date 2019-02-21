const errors = require("./errors");

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

Query.prototype.limit = function(limit) {
  this.options.limit = limit;
  return this;
};

Query.prototype.descending = function() {
  this.options.sort = "DESC";
  return this;
};

Query.prototype.ascending = function() {
  this.options.sort = "ASC";
  return this;
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

module.exports = Query;

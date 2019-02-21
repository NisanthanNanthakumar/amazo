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

module.exports = {
  ModelError: new ErrorType("Error with model", "ModelError"),
  QueryError: new ErrorType("Error with query", "QueryError"),
  ScanError: new ErrorType("Error with scan", "ScanError")
};

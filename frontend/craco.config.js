const path = require("path");
require("dotenv").config();

function makeDevServerV5Compatible(devServerConfig) {
  const {
    https,
    onAfterSetupMiddleware,
    onBeforeSetupMiddleware,
    onListening,
    setupMiddlewares,
    ...compatibleConfig
  } = devServerConfig;

  compatibleConfig.server =
    typeof https === "object" ? { type: "https", options: https } : https ? "https" : "http";

  if (onBeforeSetupMiddleware || setupMiddlewares) {
    compatibleConfig.setupMiddlewares = (middlewares, devServer) => {
      if (onBeforeSetupMiddleware) onBeforeSetupMiddleware(devServer);
      return setupMiddlewares ? setupMiddlewares(middlewares, devServer) : middlewares;
    };
  }

  compatibleConfig.onListening = (devServer) => {
    devServer.close ??= (callback) => devServer.stopCallback(callback);
    if (onListening) onListening(devServer);
    if (onAfterSetupMiddleware) onAfterSetupMiddleware(devServer);
  };

  return compatibleConfig;
}

module.exports = {
  webpack: {
    alias: { "@": path.resolve(__dirname, "src") },
    configure: (config) => {
      config.watchOptions = { ...config.watchOptions, ignored: ["**/node_modules/**", "**/.git/**"] };
      return config;
    },
  },
  devServer: (devServerConfig) => makeDevServerV5Compatible(devServerConfig),
};

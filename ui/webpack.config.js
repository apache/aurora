var path = require('path');
var webpack = require('webpack');

var SOURCE_PATH = path.resolve(__dirname, 'src/main/js');
var EXTENSION_PATH = path.resolve(__dirname, 'plugin/js');

module.exports = {
  devtool: 'source-map',
  entry: './src/main/js/index',
  output: {
    path:  path.resolve(__dirname, '../src/main/resources/scheduler/assets/js'),
    filename: 'bundle.js',
    publicPath: '/'
  },
  resolve: {
    extensions: [ '.js' ],
    modules: [EXTENSION_PATH, SOURCE_PATH, 'node_modules']
  },
  module: {
    rules: [{
        test: /\.js?$/,
        loaders: ['babel-loader'],
        include: [EXTENSION_PATH, SOURCE_PATH]
      }, {
      test: /\.scss$/,
      use: [{
        loader: 'style-loader'
      }, {
        loader: 'css-loader'
      }, {
        loader: 'sassjs-loader',
        options: {
          includePaths: [path.resolve(__dirname, 'src/main/sass')]
        }
      }]
    }, {
      test: /\.css$/,
      use: ['style-loader', 'css-loader']
    }, {
      test: /\.(png|woff|woff2|eot|ttf|svg)$/,
      loader: 'url-loader?limit=150000'
    }]
  }
};

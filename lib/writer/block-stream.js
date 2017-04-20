/*
 * Copyright 2017 resin.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const stream = require('stream');
const debug = require('debug')('block-stream');

class BlockStream extends stream.Transform {

  constructor(options) {

    options = Object.assign({}, BlockStream.defaults, options);
    options.readableObjectMode = true;

    super(options);

    this.blockSize = options.blockSize;
    this.chunkSize = options.chunkSize;
    this.bytesRead = 0;
    this.bytesWritten = 0;

    this._buffers = [];
    this._bytes = 0;

    debug('new %j', options);

  }

  _transform(chunk, _, next) {

    this.bytesRead += chunk.length;

    if (this._bytes === 0 && chunk.length >= this.chunkSize) {
      if (chunk.length % this.blockSize === 0) {
        this.bytesWritten += chunk.length;
        this.push(chunk);
        return next();
      }
    }

    this._buffers.push(chunk);
    this._bytes += chunk.length;

    if (this._bytes >= this.chunkSize) {
      var block = Buffer.concat(this._buffers);
      this._buffers.length = 0;
      this._bytes = 0;
      var length = Math.floor(block.length / this.blockSize) * this.blockSize;
      if (block.length !== length) {
        this._buffers.push(block.slice(length));
        this._bytes += block.length - length;
        block = block.slice(0,length);
      }
      this.bytesWritten += block.length;
      this.push(block);
    }

    next();

  }

  _flush(done) {

    if (!this._bytes) {
      return done();
    }

    var length = Math.ceil(this._bytes / this.blockSize) * this.blockSize;
    var block = Buffer.alloc(length);
    var offset = 0;

    for (var i = 0; i < this._buffers.length; i++) {
      this._buffers[i].copy(block, offset);
      offset += this._buffers[i].length;
    }

    this.push(block);
    done();

  }

}

BlockStream.defaults = {
  blockSize: 512,
  chunkSize: 64 * 1024
};

module.exports = BlockStream;

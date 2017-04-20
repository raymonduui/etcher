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

const _ = require('lodash');
const stream = require('stream');
const fs = require('fs');
const debug = require('debug')('block-read-stream');

class BlockReadStream extends stream.Readable {

  constructor(options) {

    options = Object.assign({}, BlockReadStream.defaults, options);
    options.objectMode = true;

    debug('block-read-stream %j', options);

    super(options);

    this.fs = options.fs;
    this.fd = options.fd;
    this.path = options.path;
    this.flags = options.flags;
    this.mode = options.mode;
    this.end = options.end || Infinity;
    this.autoClose = options.autoClose;

    this.position = options.start || 0;
    this.bytesRead = 0;

    this.closed = false;
    this.destroyed = false;

    this.once('end', function() {
      if (this.autoClose) {
        this.close();
      }
    });

    this.open();

  }

  _read() {

    const toRead = this.end - this.position;

    if (toRead <= 0) {
      this.push(null);
      return;
    }

    const length = Math.min(64 * 1024, Math.max(512, toRead));
    const buffer = Buffer.alloc(length);

    // debug('read', toRead, length)
    const onRead = (error, bytesRead) => {

      if (!error && bytesRead !== length) {
        error = new Error(`Bytes read mismatch: ${bytesRead} != ${length}`);
      }

      if (error) {
        if (this.autoClose) {
          this.destroy();
        }
        this.emit('error', error);
        return;
      }

      this.bytesRead += bytesRead;
      this.push(buffer);

    };

    this.fs.read(this.fd, buffer, 0, length, this.position, onRead);
    this.position += length;

  }

  open() {

    debug('open');

    if (!_.isNil(this.fd)) {
      return;
    }

    this.fs.open(this.path, this.flags, this.mode, (error, fd) => {
      if (error) {
        if (this.autoClose) {
          this.destroy();
        }
        this.emit('error', error);
      } else {
        this.fd = fd;
        this.emit('open', fd);
      }
    });

  }

  close(callback) {

    debug('close');

    if (callback) {
      this.once('close', callback);
    }

    if (this.closed || _.isNil(this.fd)) {
      if (_.isNil(this.fd)) {
        this.once('open', () => {
          this.close();
        });
      } else {
        process.nextTick(() => {
          this.emit('close');
        });
      }
      return;
    }

    this.closed = true;

    this.fs.close(this.fd, (error) => {
      if (error) {
        this.emit('error', error);
      } else {
        this.emit('close');
      }
    });

    this.fd = null;

  }

  destroy() {

    debug('destroy');

    if (this.destroyed) {
      return;
    }
    this.destroyed = true;
    this.close();
  }

}

BlockReadStream.defaults = {
  fs,
  fd: null,
  path: null,
  flags: 'r',
  mode: 0o666,
  autoClose: true
};

module.exports = BlockReadStream;

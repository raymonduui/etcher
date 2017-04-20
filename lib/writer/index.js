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
const Pipage = require('pipage');
const BlockMap = require('blockmap');
const BlockStream = require('./block-stream');
const BlockWriteStream = require('./block-write-stream');
const BlockReadStream = require('./block-read-stream');
const ChecksumStream = require('./checksum-stream');
const ProgressStream = require('progress-stream');
const debug = require('debug')('image-writer');
const EventEmitter = require('events').EventEmitter;

module.exports = class ImageWriter extends EventEmitter {

  constructor(options) {

    super();

    this.options = options;

    this.source = null;
    this.pipeline = null;
    this.target = null;

    this.hadError = false;

    this.bytesRead = 0;
    this.bytesWritten = 0;
    this.checksum = {};

  }

  write() {

    this.hadError = false;

    this.createWritePipeline(this.options)
      .on('checksum', (checksum) => {
        debug('write:checksum', checksum);
        this.checksum = checksum;
      })
      .on('error', (error) => {
        this.hadError = true;
        this.emit('error', error);
      });

    this.target.on('finish', () => {
      this.bytesRead = this.source.bytesRead;
      this.bytesWritten = this.target.bytesWritten;
      if (!this.options.verify) {
        this.finish();
      } else {
        this.verify();
      }
    });

    return this;

  }

  verify() {

    this.createVerifyPipeline(this.options)
      .on('error', (error) => {
        this.hadError = true;
        this.emit('error', error);
      })
      .on('checksum', (checksum) => {
        debug('verify:checksum', checksum);
        // TODO: Verify checksums
      })
      .on('end', () => {
        debug('verify:end');
        this.finish();
      });

    return this;

  }

  finish() {
    this.emit('finish', {
      bytesRead: this.bytesRead,
      bytesWritten: this.bytesWritten,
      checksum: this.checksum
    });
  }

  abort() {
    if (this.source) {
      this.emit('abort');
      this.source.destroy();
    }
  }

  createWritePipeline(options) {

    const pipeline = new Pipage();

    const image = options.image;
    const source = image.stream;
    const progressOptions = {
      length: image.size.original,
      type: 'write',
      time: 500
    };

    let progressStream = null;

    // If the final size is an estimation,
    // use the original source size for progress metering
    if (image.size.final.estimation) {
      progressStream = new ProgressStream(progressOptions);
      pipeline.append(progressStream);
    }

    const isPassThrough = image.transform instanceof stream.PassThrough;

    // If the image transform is a pass-through,
    // ignore it to save on the overhead
    if (image.transform && !isPassThrough) {
      pipeline.append(image.transform);
    }

    // If the final size is known precisely and we're not
    // using block maps, then use the final size for progress
    if (!image.size.final.estimation && !image.bmap) {
      progressOptions.length = image.size.final.value;
      progressStream = new ProgressStream(progressOptions);
      pipeline.append(progressStream);
    }

    if (image.bmap) {
      const blockMap = BlockMap.parse(image.bmap);
      debug('write:bmap', blockMap);
      // TODO: Remove the block-stream here once
      // https://github.com/resin-io-modules/blockmap/pull/27
      // is merged & published
      pipeline.append(new BlockStream());
      pipeline.append(new BlockMap.FilterStream(blockMap, {
        objectMode: true
      }));
      progressOptions.length = blockMap.mappedBlockCount * blockMap.blockSize;
      progressStream = new ProgressStream(progressOptions);
      pipeline.append(progressStream);
    } else {
      debug('write:blockstream');
      const checksumStream = new ChecksumStream({
        algorithms: options.checksumAlgorithms
      });
      pipeline.append(checksumStream);
      pipeline.bind(checksumStream, 'checksum');
      pipeline.append(new BlockStream());
    }

    const target = new BlockWriteStream({
      fd: options.fd,
      path: options.path,
      flags: options.flags,
      mode: options.mode,
      autoClose: false
    });

    // Pipeline.bind(progressStream, 'progress');
    progressStream.on('progress', (state) => {
      state.type = 'write';
      this.emit('progress', state);
    });

    pipeline.bind(source, 'error');
    pipeline.bind(target, 'error');

    source.pipe(pipeline)
      .pipe(target);

    this.source = source;
    this.pipeline = pipeline;
    this.target = target;

    return pipeline;

  }

  createVerifyPipeline(options) {

    const pipeline = new Pipage({
      objectMode: true
    });

    const source = new BlockReadStream({
      fd: options.fd,
      path: options.path,
      flags: options.flags,
      mode: options.mode,
      autoClose: false,
      end: this.bytesWritten
    });

    const progressStream = new ProgressStream({
      length: this.bytesWritten,
      type: 'verify',
      time: 500
    });

    this.target = null;
    this.source = source;
    this.pipeline = pipeline;

    if (options.image.bmap) {
      debug('verify:bmap');
      const blockMap = BlockMap.parse(options.image.bmap);
      // TODO: Maybe use BlockMap.ReadStream as source here once
      // https://github.com/resin-io-modules/blockmap/pull/26
      // is published
      pipeline.append(BlockMap.createFilterStream(blockMap));
    } else {
      const checksumStream = new ChecksumStream({
        algorithms: options.checksumAlgorithms
      });
      pipeline.append(checksumStream);
      pipeline.bind(checksumStream, 'checksum');
    }

    pipeline.prepend(progressStream);
    pipeline.bind(source, 'error');

    progressStream.on('progress', (state) => {
      state.type = 'verify';
      this.emit('progress', state);
    });

    return source.pipe(pipeline).resume();

  }

};

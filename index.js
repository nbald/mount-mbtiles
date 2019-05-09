var constants = require("constants");

var fs = require("fs"),
    path = require("path");

var fuse = require('fuse-bindings'),
    MBTiles = require('@mapbox/mbtiles'),
    SmartBuffer = require('smart-buffer').SmartBuffer;

// TODO require these arguments
var args = process.argv.slice(2),
    filename = path.resolve(args.shift()),
    mountPoint = path.resolve(args.shift());

var tileStore;

var filesBeingWritten = {};

/**
 * Convert a path into XYZ coords.
 */
var lookup = function(path) {
  var parts = path.split("/", 4);

  if (parts[1]) {
    var z = Number(parts[1]);
  }

  if (parts[2]) {
    var x = Number(parts[2]);
  }

  if (parts[3]) {
    var y;
    var matches = parts[3].match(/^(\d+)\.png$/);
    if (matches) {
      y = Number(matches[1]);
    } else {
      y = NaN;
    }
  }

  return {
    z: z,
    x: x,
    y: y
  };
};

/**
 * getattr() system call handler.
 */
var getattr = function(path, callback) {
  var stat = {
    // atime: new Date(),
    // mtime: new Date(),
    // ctime: new Date(),
    uid: process.getuid ? process.getuid() : 0,
    gid: process.getgid ? process.getgid() : 0
  };

  if (path === '/') {
    stat.size = 4096; // standard size of a directory
    stat.mode = 040755; // directory with 755 permissions
    return callback(0, stat);
  } else if (path in filesBeingWritten) {
    stat.mode = 0100644;
    stat.size = filesBeingWritten[path].length
    return callback(0, stat);
  }

  var info = lookup(path);
  if (
    Number.isNaN(info.z) ||
    Number.isNaN(info.y) ||
    Number.isNaN(info.x)) {
    return callback(-constants.ENOENT);
  }

  var isADirectory = true;

  var conditions = [ '1' ];

  if (info.x !== undefined) {
    conditions.push(`tile_column = ${info.x}`)
  }
  if (info.y !== undefined) {
    // Flip Y coordinate because MBTiles files are TMS.
    var y = (1 << info.z) - 1 - info.y;
    conditions.push(`tile_row = ${y}`)
    isADirectory = false;
  }
  if (info.z !== undefined) {
    conditions.push(`zoom_level = ${info.z}`)
  }

  var where = conditions.join(' and ')

  var sql = `
    SELECT length(tile_data) as size
    FROM tiles
    WHERE ${where}
    LIMIT 1
  `

  tileStore._db.get(sql, function(err, row) {
    if (row === undefined) return callback(-constants.ENOENT);
    if (isADirectory) {
      stat.size = 4096; // standard size of a directory
      stat.mode = 040755; // directory with 755 permissions
    } else {
      stat.size = row.size;
      stat.mode = 0100644; // file with 444 permissions
    }
    if (err) {
      // throw err;
      // the table may not exist yet if the mbtiles file is new
      console.error(err);
      return callback(-constants.ENOENT);
    }
    callback(0, stat);
  });
};

var readdir = function(path, callback) {
  var info = lookup(path);
  if (info.y !== undefined) {
    callback(-constants.EINVAL); // this is a file
    return;
  }

  if (info.x !== undefined) {
    var query = tileStore._db.prepare("SELECT max(length(tile_data)) as max_size, tile_row FROM tiles WHERE tile_column = ? AND zoom_level = ? GROUP BY tile_row", function(err) {
      if (err) {
        console.warn("readdir:", err, info);
        callback(-constants.EINVAL);
        return;
      }

      query.all(info.x, info.z, function(err, rows) {
        if (!rows) return callback(0, []);
        var names = rows
        .filter(function(x) {
          return x.max_size > 0;
        })
        .map(function(x) {
          var y = (1 << info.z) - 1 - x.tile_row;
          // TODO get format from info
          return String(y) + ".png";
        });

        callback(0, names);
      });
    });

    return;
  }

  if (info.z !== undefined) {
    var query = tileStore._db.prepare("SELECT DISTINCT tile_column FROM tiles WHERE zoom_level = ?", function(err) {
      if (err) {
        console.warn(err, info);
        callback(-constants.EINVAL);
        return;
      }

      query.all(info.z, function(err, rows) {
        if (!rows) return callback(0, []);
        var names = rows.map(function(x) {
          return String(x.tile_column);
        });
        if (names.length === 0) {
          return callback(fuse.ENOENT);
        }

        callback(0, names);
      });
    });

    return;
  }

  // TODO use (cached) getInfo to determine this
  tileStore._db.all("SELECT DISTINCT zoom_level FROM tiles", function(err, rows) {
    if (!rows) return callback(0, []);
    var names = rows.map(function(x) {
      return String(x.zoom_level);
    });

    callback(0, names);
  });
};

/**
 * open() system call handler.
 */
var open = function(path, flags, callback) {
  // TODO open for writing
  var err = 0;
  var info = lookup(path);

  if (info.y === undefined) {
    err = -constants.ENOENT;
  }

  callback(err);
};

/**
 * read() system call handler.
 */
var read = function(path, fh, buf, len, offset, callback) {
  var err = 0;
  var info = lookup(path);
  var maxBytes;
  var data;

  if (info.y !== undefined) {
    tileStore.getTile(info.z, info.x, info.y, function(err, tile, options) {
      if (err) {
        console.warn(err, info);
        callback(-constants.ENOENT);
        return;
      }

      if (offset < tile.length) {
        maxBytes = tile.length - offset;
        if (len > maxBytes) {
          len = maxBytes;
        }
        tile.copy(buf, 0, offset, offset + len);
        err = len;
      }

      callback(err);
    });
  } else {
    callback(-constants.EPERM); // a directory
  }
};

/**
 * release() system call handler.
 */
var release = function(path, fh, callback) {
  if (path in filesBeingWritten) {
    return commitWrite(path, callback)
  }
  callback(0);
};

var init = function(callback) {
  new MBTiles(filename + '?mode=rwc', function(err, mbtiles) {
    if (err) throw err;
    tileStore = mbtiles;
    mbtiles.getInfo(function(err, info) {
      if (err) throw err;

      console.log("tileStore initialized.");
      console.log(info);
      callback();
    });
  });
};

var destroy = function(callback) {
  tileStore.close(callback);
};

var statfs = function(path, callback) {
  return callback(0, {
    bsize: 1000000,
    frsize: 1000000,
    blocks: 1000000,
    bfree: 1000000,
    bavail: 1000000,
    files: 1000000,
    ffree: 1000000,
    favail: 1000000,
    fsid: 1000000,
    flag: 1000000,
    namemax: 1000000
  });
};

var mkdir = function(path, mode, callback) {
  // TODO do we need to check if path already exists?
  var info = lookup(path);

  if (
    Number.isNaN(info.z) ||
    Number.isNaN(info.y) ||
    Number.isNaN(info.x)
  ) {
    return callback(-constants.EINVAL);
  }

  info.x = Number.isInteger(info.x) ? info.x : 0;
  info.y = Number.isInteger(info.y) ? info.y : 0;
  info.z = Number.isInteger(info.z) ? info.z : 0;

  const emptyTile = new Buffer(0);

  tileStore.startWriting(function(err) {
    if (err) throw err;
    tileStore.putTile(info.z, info.x, info.y, emptyTile, function(err) {
      if (err) throw err;
      tileStore.stopWriting(function(err) {
        if (err) throw err;
        return callback(0)
      });
    })
  });
}

var unlink = function(path, callback) {
  var info = lookup(path);
  if (
    Number.isNaN(info.z) ||
    Number.isNaN(info.y) ||
    Number.isNaN(info.x)
  ) {
    return callback(-constants.ENOENT);
  }

  var conditions = [ '1' ];

  if (info.x !== undefined) {
    conditions.push(`tile_column = ${info.x}`)
  }
  if (info.y !== undefined) {
    // Flip Y coordinate because MBTiles files are TMS.
    var y = (1 << info.z) - 1 - info.y;
    conditions.push(`tile_row = ${y}`)
  }
  if (info.z !== undefined) {
    conditions.push(`zoom_level = ${info.z}`)
  }

  // BUG the dir gets deleted when tile 0.png is deleted
  var where = conditions.join(' and ')
  var query = `
    BEGIN TRANSACTION;
    /* delete orphaned images */
    DELETE FROM images WHERE tile_id IN (
      SELECT
       full_set.tile_id
      FROM map as full_set
      JOIN
      (
       SELECT tile_id, count(tile_id) AS affected_count FROM map
       WHERE ${where}
       GROUP BY tile_id
      ) AS affected
      ON affected.tile_id = full_set.tile_id
      GROUP BY full_set.tile_id
      HAVING count(full_set.tile_id) - affected_count <= 0
    );
    /* delete tiles records */
    DELETE FROM map WHERE ${where};
    COMMIT;
  `;

  tileStore.startWriting(function(err) {
    if (err) throw err;
    tileStore._db.exec(query, function(err) {
      if (err) throw err;
      tileStore.stopWriting(function(err) {
        if (err) throw err;
        return callback(0)
      });
    })
  });
};

var create = function (path, mode, callback) {
  var info = lookup(path);
  if (
    !Number.isInteger(info.z) ||
    !Number.isInteger(info.y) ||
    !Number.isInteger(info.x)
  ) {
    return callback(-constants.EINVAL)
  }
  filesBeingWritten[path] = new SmartBuffer();
  callback(0);
}

var write = function (path, fh, buf, len, offset, callback) {
  if (!(path in filesBeingWritten)) {
    return callback(-constants.EINVAL)
  }
  filesBeingWritten[path].writeBuffer(buf.slice(0, len), offset)
  callback(len)
}

var commitWrite = function (path, callback) {
  var end = function (err, status) {
    filesBeingWritten[path].destroy();
    delete filesBeingWritten[path];
    if (err) throw err;
    callback(status)
  }

  var info = lookup(path);

  tileStore.startWriting(function(err) {
    if (err) return end(err);
    tileStore.putTile(info.z, info.x, info.y, filesBeingWritten[path].toBuffer(), function(err) {
      if (err) return end(err);
      tileStore.stopWriting(function(err) {
        if (err) return end(err);
        return end(null, 0)
      });
    })
  });
}

var truncate = function (path, size, callback) {
  var info = lookup(path);
  if (
    !Number.isInteger(info.z) ||
    !Number.isInteger(info.y) ||
    !Number.isInteger(info.x) ||
    size !== 0
  ) {
    return callback(-constants.EINVAL)
  }

  create(path, 0100644, callback);
}

var catchErrors = function (originalFunction) {
  return function () {
    try {
      originalFunction.apply(null, arguments);
    } catch (err) {
      var callback = arguments[arguments.length - 1]
      console.error(err);
      callback(-constants.EIO)
    }
  }
}

var options = {
  force: true,
  getattr: catchErrors(getattr),
  readdir: catchErrors(readdir),
  open: catchErrors(open),
  truncate: catchErrors(truncate),
  read: catchErrors(read),
  write: catchErrors(write),
  release: catchErrors(release),
  create: catchErrors(create),
  unlink: catchErrors(unlink),
  // rename: catchErrors(rename),
  mkdir: catchErrors(mkdir),
  rmdir: catchErrors(unlink),
  init: init,
  destroy: catchErrors(destroy),
  statfs: catchErrors(statfs)
};

fs.mkdir(mountPoint, function(err) {
  if (err && err.code !== "EEXIST") {
    throw err;
  }

  fuse.mount(mountPoint, options, function (err) {
    if (err) throw err
    console.log('filesystem mounted on ' + mountPoint)
  })

});

process.on('SIGINT', function () {
  fuse.unmount(mountPoint, function (err) {
    if (err) {
      console.log('filesystem at ' + mountPoint + ' not unmounted', err)
    } else {
      console.log('filesystem at ' + mountPoint + ' unmounted')
    }
  })
})

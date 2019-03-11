/*
redport - v0.1.1

Written by Federico Pereiro (fpereiro@gmail.com) and released into the public domain.

Please refer to readme.md to read the annotated source (but not yet!).
*/

var clog = console.log;

if (process.argv.length !== 4) return clog ('Usage: node redport FROM TO');

var from = process.argv [2], to = process.argv [3];

// *** EXPORT MODE ***

if (! isNaN (parseInt (from))) {
   if (! to.match (/.json$/)) to += '.json';
   var rl = require ('readline').createInterface (process.stdin, process.stdout);
   rl.question ('Do you want to export Redis DB #' + from + ' onto file ' + to + '? (type `yes` to confirm)\n', function (answer) {
      rl.close ();
      if (answer !== 'yes') return clog ('Operation aborted by user.');

      var redisbin = require ('redis').createClient ({db: from, return_buffers: true}).on ('error', function (error) {
         clog ('Redis error', error);
         process.exit (1);
      });
      var redis = require ('redis').createClient ({db: from}).on ('error', function (error) {
         clog ('Redis error', error);
         process.exit (1);
      });

      var cursor = 0, seenkeys = {}, size = 0, T = Date.now ();
      var loop = function (firstloop) {
         redis.scan (cursor, 'MATCH', '*', function (error, data) {
            if (error) return clog ('Redis error', error);
            cursor = data [0];
            var keys = [], multi = redis.multi ();
            for (var i = 0; i < data [1].length; i++) {
               if (seenkeys [data [1] [i]]) return;
               seenkeys [data [1] [i]] = true;
               keys.push (data [1] [i]);
               multi.ttl (data [1] [i]);
            }
            var t = Date.now ();
            multi.exec (function (error, ttls) {
               if (error) return clog ('Redis error', error);
               if (keys.length === 0 && cursor === '0') {
                  clog ('Exported ' + Object.keys (seenkeys).length + ' keys (' + size + ' bytes) in ' + Math.round ((Date.now () - T) / 1000) + ' seconds');
                  return process.exit (0);
               }
               var output = '', counter = keys.length;
               for (var j = 0; j < keys.length; j++) {
                  (function () {
                     var k = j;
                     redisbin.dump (keys [k], function (error, dump) {
                        if (counter === false) return;
                        if (error) {
                           counter = false;
                           return clog ('Redis error', error);
                        }
                        output += JSON.stringify ({n: keys [k], t: ttls [k] > -1 ? (t + 1000 * (ttls [k])) : undefined, d: dump.toString ('base64')}) + '\n';
                        if (--counter > 0) return;
                        require ('fs').writeFile (to, output, {flag: firstloop ? 'w' : 'a'}, function (error) {
                           if (error) return clog ('FS error', error);
                           size += output.length;
                           if (cursor === '0') {
                              clog ('Exported ' + Object.keys (seenkeys).length + ' keys (' + size + ' bytes) in ' + Math.round ((Date.now () - T) / 1000) + ' seconds');
                              return process.exit (0);
                           }
                           loop ();
                        });
                     });
                  }) ();
               }
            });
         });
      }
      loop (true);
   });
}

// *** IMPORT MODE ***

else if (! isNaN (parseInt (to))) {
   if (! from.match (/.json$/)) from += '.json';
   var rl = require ('readline').createInterface (process.stdin, process.stdout);
   rl.question ('Do you want to import file ' + from + ' onto Redis DB #' + to + '? (type `yes` to confirm - THIS WILL DELETE THE REDIS DB #' + to + ')\n', function (answer) {
      rl.close ();
      if (answer !== 'yes') return clog ('Operation aborted by user.');

      var redis = require ('redis').createClient ({db: to}).on ('error', function (error) {
         clog ('Redis error', error);
         process.exit (1);
      });

      redis.flushdb (function (error) {
         if (error) return clog ('Redis error', error);

         rl = require ('readline').createInterface (require ('fs').createReadStream (from));
         rl.on ('error', function (error) {
            clog ('FS error', error);
            process.exit (1);
         });
         var T = Date.now (), count = 0, pending = 0;
         rl.on ('line', function (line) {
            pending++, count++;
            line = JSON.parse (line);
            var t = Date.now ();
            if (line.t !== undefined && line.t < t) return pending--;
            redis.restore (line.n, line.t === undefined ? 0 : Math.ceil ((line.t - t) / 1000), Buffer.from (line.d, 'base64'), function (error) {
               if (error) return clog ('Redis error', error);
               pending--;
            });
         });
         var check;
         rl.on ('close', function () {
            check = setInterval (function () {
               if (pending > 0) return;
               clearInterval (check);
               clog ('Imported ' + count + ' keys in ' + Math.round ((Date.now () - T) / 1000) + ' seconds');
               process.exit (0);
            }, 100);
         });
      });
   });
}

// *** NO DATABASE NUMBER DETECTED ***

else clog ('No valid Redis DB number passed (must be 0-15)');

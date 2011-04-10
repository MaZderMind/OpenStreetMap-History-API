module.exports = require('util');

// borrowed from ExtJS
//  http://code.google.com/p/extjs-public/source/browse/extjs-3.x/release/src/util/Function.js#88
module.exports.createDelegate = function(fn, obj, args, appendArgs) {
    return function() {
        var callArgs = args || arguments;
        if (appendArgs === true) {
            callArgs = Array.prototype.slice.call(arguments, 0);
            callArgs = callArgs.concat(args);
        }
        return fn.apply(obj || {}, callArgs);
    };
}


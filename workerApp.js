
var Worker = require('./worker');
w = new Worker ('someName', 'localhost', 1337, 60000, 5);
w.eat();
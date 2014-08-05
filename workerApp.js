
var Worker = require('./worker');
w = new Worker ('localhost', 1337, 'someName', 60000, 5);
w.eat();
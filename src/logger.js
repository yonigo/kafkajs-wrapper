const levelColors = {
    fatal: '\x1b[31m', // red
    error: '\x1b[31m', // red
    warn: '\x1b[33m', // yellow
    debug: '\x1b[34m' // blue
  };
  
const resetColor = '\x1b[0m';

const getCallingFile = () => {
    const originalFunc = Error.prepareStackTrace;
    let callerFile;
    try {
      const err = new Error();
      Error.prepareStackTrace = (error, stack) => stack;
      const { stack } = err;
      while (stack.length) {
        callerFile = stack.shift().getFileName();
        if (!/logger\/src\//.test(callerFile)) {
          break;
        }
      }
      // eslint-disable-next-line no-empty
    } catch (ex) {
  
    }
  
    Error.prepareStackTrace = originalFunc;
  
    // Remove dir path and extension, leave only the file name
    return callerFile.replace(/.+\/(.+)\..+/, '$1');
  };

class Logger {
  constructor() {}

  log(level, message, meta) {
    if (typeof level !== 'string' || typeof message !== 'string') {
      console.log('Couldn\'t log message', level, message, meta);
      return;
    }

    const color = levelColors[level] || '';
    const dateString = new Date().toISOString().replace('T', ' ').replace('Z', '');
    const formattedMessage = `${color}${dateString}`
      + ` ${level.toUpperCase()} ${getCallingFile()}: ${message}`;

    console.log(formattedMessage, meta && meta.error ? meta.error : '', resetColor);
  }

  debug(message, meta) {
    this.log('debug', message, meta);
  }

  info(message, meta) {
    this.log('info', message, meta);
  }

  warn(message, meta) {
    this.log('warn', message, meta);
  }

  error(message, meta) {
    this.log('error', message, meta);
  }

  fatal(message, meta) {
    this.log('fatal', message, meta);
  }
}

module.exports = new Logger();

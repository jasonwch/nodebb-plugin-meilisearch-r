'use strict';

const plugin = {};

require('./lib/state')(plugin);
require('./lib/client')(plugin);
require('./lib/reindex')(plugin);
require('./lib/hooks/posts')(plugin);
require('./lib/hooks/topics')(plugin);
require('./lib/hooks/messages')(plugin);
require('./lib/search')(plugin);
require('./lib/chat-search-global')(plugin);
require('./lib/settings')(plugin);
require('./lib/routes')(plugin);

module.exports = plugin;

#pragma once
#include <iostream>
#include "utils/commandLineParser.hpp"

#include "driver.h"
 #include "ittnotify.h"

int main(int argc, char** argv) {
   __itt_pause();
  commandLineParser& parser = commandLineParser::get_instance();
  parser.parse("/path/to/the/config.cfg");

  wrapper::execute(parser.get_driver_config());
  return 0;
}

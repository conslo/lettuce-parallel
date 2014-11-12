# -*- coding: utf-8 -*-
# <Lettuce - Behaviour Driven Development for python>
# Copyright (C) <2010-2012>  Gabriel Falcão <gabriel@nacaolivre.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__version__ = version = '0.2.20'

release = 'kryptonite'

import os
import sys
import traceback
import multiprocessing
try:
    from imp import reload
except ImportError:
    # python 2.5 fallback
    pass

import random

from lettuce.core import Feature, TotalResult

from lettuce.terrain import after
from lettuce.terrain import before
from lettuce.terrain import world

from lettuce.decorators import step, steps
from lettuce.registry import call_hook
from lettuce.registry import STEP_REGISTRY
from lettuce.registry import CALLBACK_REGISTRY
from lettuce.exceptions import StepLoadingError
from lettuce.plugins import (
    xunit_output,
    subunit_output,
    autopdb,
    smtp_mail_queue,
)
from lettuce import fs
from lettuce import exceptions

try:
    from colorama import init as ms_windows_workaround
    ms_windows_workaround()
except ImportError:
    pass


__all__ = [
    'after',
    'before',
    'step',
    'steps',
    'world',
    'STEP_REGISTRY',
    'CALLBACK_REGISTRY',
    'call_hook',
]

try:
    terrain = fs.FileSystem._import("terrain")
    reload(terrain)
except Exception, e:
    if not "No module named terrain" in str(e):
        string = 'Lettuce has tried to load the conventional environment ' \
            'module "terrain"\nbut it has errors, check its contents and ' \
            'try to run lettuce again.\n\nOriginal traceback below:\n\n'

        sys.stderr.write(string)
        sys.stderr.write(exceptions.traceback.format_exc(e))
        raise SystemExit(1)


class Runner(object):
    """ Main lettuce's test runner

    Takes a base path as parameter (string), so that it can look for
    features and step definitions on there.
    """
    def __init__(self, base_path, scenarios=None, verbosity=0, random=False,
                 enable_xunit=False, xunit_filename=None,
                 enable_subunit=False, subunit_filename=None,
                 tags=None, failfast=False, auto_pdb=False,
                 smtp_queue=None):

        """ lettuce.Runner will try to find a terrain.py file and
        import it from within `base_path`
        """

        self.tags = tags
        self.single_feature = None

        if os.path.isfile(base_path) and os.path.exists(base_path):
            self.single_feature = base_path
            base_path = os.path.dirname(base_path)

        sys.path.insert(0, base_path)
        self.loader = fs.FeatureLoader(base_path)
        self.verbosity = verbosity
        self.scenarios = scenarios and map(int, scenarios.split(",")) or None
        self.failfast = failfast
        if auto_pdb:
            autopdb.enable(self)

        sys.path.remove(base_path)

        if verbosity is 0:
            from lettuce.plugins import non_verbose as output
        elif verbosity is 1:
            from lettuce.plugins import dots as output
        elif verbosity is 2:
            from lettuce.plugins import scenario_names as output
        elif verbosity is 3:
            from lettuce.plugins import shell_output as output
        else:
            from lettuce.plugins import colored_shell_output as output

        self.random = random

        if enable_xunit:
            xunit_output.enable(filename=xunit_filename)
        if smtp_queue:
            smtp_mail_queue.enable()

        if enable_subunit:
            subunit_output.enable(filename=subunit_filename)

        reload(output)

        self.output = output

    def load_features_files(self):
        """ Find and return features files
        """
        if self.single_feature:
            return [self.single_feature]
        else:
            features_files = self.loader.find_feature_files()
            if self.random:
                random.shuffle(features_files)
            return features_files

    def run(self):
        """ Find and load step definitions, and them find and load
        features under `base_path` specified on constructor
        """
        results = []
        features_files = self.load_features_files()

        if not features_files:
            self.output.print_no_features_found(self.loader.base_dir)
            return

        # only load steps if we've located some features.
        # this prevents stupid bugs when loading django modules
        # that we don't even want to test.
        try:
            self.loader.find_and_load_step_definitions()
        except StepLoadingError, e:
            print "Error loading step definitions:\n", e
            return

        call_hook('before', 'all')
        # So parallel stuff works in serial
        call_hook('before', 'batch', 1)

        failed = False
        try:
            for filename in features_files:
                feature = Feature.from_file(filename)
                results.append(
                    feature.run(self.scenarios,
                                tags=self.tags,
                                random=self.random,
                                failfast=self.failfast))

        except exceptions.LettuceSyntaxError, e:
            sys.stderr.write(e.msg)
            failed = True
        except:
            if not self.failfast:
                e = sys.exc_info()[1]
                print "Died with %s" % str(e)
                traceback.print_exc()
            else:
                print
                print ("Lettuce aborted running any more tests "
                       "because was called with the `--failfast` option")

            failed = True

        finally:
            total = TotalResult(results)
            total.output_format()
            # So parallel stuff works in serial
            call_hook('after', 'batch')
            call_hook('after', 'all', total)

            if failed:
                raise SystemExit(2)

            return total



class ShutdownWork(object):
    """Exists only to identify an object as a shutdown signal for parallel workers"""
    pass


class TestError(object):
    """A distinct error object for parallel workers to pass back so (test framework) errors can be printed at the end"""
    def __init__(self, exception, id):
        self.exception = exception
        self.id = id


class ParallelRunner(Runner):
    """ An implementation of the Runner that does stuff in parallel, woo!
    """
    def __init__(self, *args, **kwargs):
        # Error on things that don't work yet
        if kwargs.pop('auto_pdb', None):
            # TODO: this could maybe be implemented with qdb: https://github.com/quantopian/qdb
            raise NotImplementedError("auto_pdb when running in parallel doesn't do what you think it does")
        if kwargs.pop('failfast', None):
            # TODO: doable if we wrap the results returned by the threads in objects that contain exception information
            raise NotImplementedError("failfast has not been implemented in parallel")
        if kwargs.pop('smtp_queue', None):
            # TODO: idfk what this even does
            raise NotImplementedError("smtp_queue has not been implemented in parallel")

        self.parallel = kwargs.pop('parallel')
        super(ParallelRunner, self).__init__(*args, **kwargs)

    def work(self, id, input_queue, output_queue):
        """ This is the method that runs in a separate process/thread for processing features

        :param id: an int, representing a unique (per worker) number to pass to the hooks (so ports etc can work)
        :param input_queue: a Queue.Queue (or likewise) from which features are accepted (then .task_done() is run after
        obtaining a result). This may optionally be a 'ShutdownWork' object, to signal the worker to stop.
        :param output_queue: a Queue.Queue (or likewise) where the results from running the features are put.
        :return: None, this runs as a separate thread/process.
        """

        try:
            call_hook('before', 'batch', id)
        except:
            # We need the worker to continue on, so it doesn't hang
            pass
        while True:
            feature, args, kwargs = input_queue.get()
            if isinstance(feature, ShutdownWork):
                try:
                    call_hook('after', 'batch')
                except:
                    # We need the worker to continue shutting down
                    pass
                input_queue.task_done()
                break
            try:
                result = feature.run(*args, **kwargs)
            except Exception as e:
                result = TestError(e, id)

            finally:
                # It is important that we put the result on the output *before* calling .task_done(), because race
                # conditions.
                output_queue.put(result)
                input_queue.task_done()
        # This is more preventative than anything else, to prevent GC issues
        input_queue.close()
        output_queue.close()


    def run(self):
        features_files = self.load_features_files()

        if not features_files:
            self.output.print_no_features_found(self.loader.base_dir)
            return

        # limit the number of workers to the number of feature files, because that's how we divide work
        self.parallel = min(self.parallel, len(features_files))

        # only load steps if we've located some features.
        # this prevents stupid bugs when loading django modules
        # that we don't even want to test.
        try:
            self.loader.find_and_load_step_definitions()
        except StepLoadingError, e:
            print "Error loading step definitions:\n", e
            return

        call_hook('before', 'all')

        input_queue = multiprocessing.JoinableQueue()
        output_queue = multiprocessing.Queue()
        workers = []
        for i in xrange(self.parallel):
            worker = multiprocessing.Process(target=self.work, args=(i, input_queue, output_queue))
            workers.append(worker)
            worker.start()

        failed = False
        try:
            for filename in features_files:
                feature = Feature.from_file(filename)
                args = [self.scenarios]
                kwargs = {
                    'tags': self.tags,
                    'random': self.random,
                    'failfast': self.failfast,
                    }
                # Fire off to worker(s)
                input_queue.put((feature, args, kwargs))
        except exceptions.LettuceSyntaxError as e:
            sys.stderr.write(e.msg)
            failed = True
        except Exception as e:
            if not self.failfast:
                print("Died with {}".format(str(e)))
                traceback.print_exc()
            else:
                raise NotImplementedError("failfast has not been implemented in parallel")

            failed = True

        finally:

            # Tell our workers to shutdown when they're done.
            for _ in xrange(len(workers)):
                input_queue.put((ShutdownWork(), None, None))

            results = []

            # Wait for workers to shut down
            input_queue.join()

            # output_queue should not be added to anymore now, let's empty it.
            while not output_queue.empty():
                result = output_queue.get()
                if isinstance(result, TestError):
                    # Display testing errors
                    traceback.print_exception(type(result.exception), result.exception, result.exception.traceback)
                else:
                    results.append(result)

            # We can't terminate/join the workers before emptying the queue, because the background thread that manages the queue in each worker will still be running.
            # TODO[TJ]: Figure out why we can't just .join(), might be an implementation issue, but still.
            for worker in workers:
                worker.terminate()
                worker.join()

            total = TotalResult(results)
            total.output_format()
            call_hook('after', 'all', total)

            if failed:
                raise SystemExit(2)

            return total

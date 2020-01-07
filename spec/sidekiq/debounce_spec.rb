require 'spec_helper'
require 'sidekiq/debounce'
require 'sidekiq'

class DebouncedWorker
  include Sidekiq::Worker

  sidekiq_options debounce: true

  def perform(_a, _b); end
end

class  DebouncedByWorker
  include Sidekiq::Worker
  sidekiq_options debounce: true,
                  debounce_by: -> (args) { args[0...-1] }

  def perform(_a, _b, _ignored); end
end

describe Sidekiq::Debounce do
  before do
    stub_scheduled_set
  end

  after do
    Sidekiq.redis(&:flushdb)
  end

  let(:set) { Sidekiq::ScheduledSet.new }

  describe 'normal debouncing' do
    let(:sorted_entry) { Sidekiq::SortedEntry.new(set, 0, {jid: '54321'}.to_json) }

    it 'queues a job normally at first' do
      DebouncedWorker.perform_in(60, 'foo', 'bar')
      _(set.size).must_equal 1, 'set.size must be 1'
    end

    it 'ignores repeat jobs within the debounce time and reschedules' do
      sorted_entry.expects(:reschedule)

      DebouncedWorker.perform_in(60, 'foo', 'bar')
      DebouncedWorker.perform_in(60, 'foo', 'bar')
      _(set.size).must_equal 1, 'set.size must be 1'
    end

    it 'debounces jobs based on their arguments' do
      DebouncedWorker.perform_in(60, 'boo', 'far')
      DebouncedWorker.perform_in(60, 'foo', 'bar')
      _(set.size).must_equal 2, 'set.size must be 2'
    end

    it 'creates the job immediately when given an instant job' do
      DebouncedWorker.perform_async('foo', 'bar')
      _(set.size).must_equal 0, 'set.size must be 0'
    end
  end

  describe 'when debounce_by is set to ignore last argument' do
    let(:sorted_entry) { Sidekiq::SortedEntry.new(set, 0, {jid: '54321'}.to_json) }

    it 'queues a job normally at first' do
      DebouncedByWorker.perform_in(60, 'foo', 'bar', 1)
      _(set.size).must_equal 1, 'set.size must be 1'
    end

    it 'ignores repeat jobs within the debounce time and reschedules' do
      sorted_entry.expects(:reschedule)

      DebouncedByWorker.perform_in(60, 'foo', 'bar', 2)
      DebouncedByWorker.perform_in(60, 'foo', 'bar', 3)
      _(set.size).must_equal 1, 'set.size must be 1'
    end

    it 'debounces jobs based on their arguments' do
      DebouncedByWorker.perform_in(60, 'boo', 'far', 4)
      DebouncedByWorker.perform_in(60, 'foo', 'bar', 5)
      _(set.size).must_equal 2, 'set.size must be 2'
    end

    it 'creates the job immediately when given an instant job' do
      DebouncedByWorker.perform_async('foo', 'bar', 6)
      _(set.size).must_equal 0, 'set.size must be 0'
    end

  end

  def stub_scheduled_set
    set.stubs(:find_job).returns(sorted_entry)
    Sidekiq::Debounce.any_instance.stubs(:scheduled_set).returns(set)
  end
end

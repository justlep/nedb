import './utils.js';
import {generateUid} from '../lib/customUtils.js';


describe('customUtils', function () {

  describe('uid', function () {

    it('Generates a string of the expected length', function () {
      generateUid(3).length.should.equal(3);
      generateUid(16).length.should.equal(16);
      generateUid(42).length.should.equal(42);
      generateUid(1000).length.should.equal(1000);
    });

    // Very small probability of conflict
    it('Generated uids should not be the same', function () {
      generateUid(56).should.not.equal(generateUid(56));
    });

  });

});

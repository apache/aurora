import React from 'react';
import { Link } from 'react-router-dom';

export default function Navigation({ fluid }) {
  return (
    <nav className='navbar'>
      <div className={fluid ? 'container-fluid' : 'container'}>
        <div className='navbar-header'>
          <Link className='navbar-brand' to='/scheduler'>
            <img alt='Brand' src='/assets/images/aurora_logo_white.png' />
          </Link>
        </div>
        <ul className='nav navbar-nav navbar-right'>
          <li><Link to='/updates'>updates</Link></li>
        </ul>
      </div>
    </nav>
  );
}

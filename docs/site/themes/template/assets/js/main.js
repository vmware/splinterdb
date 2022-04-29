"use strict";

function mobileNavToggle() {
    var menu = document.getElementById('mobile-menu').parentElement;
    menu.classList.toggle('mobile-menu-visible');
}

function docsVersionToggle() {
    var menu = document.getElementById('dropdown-menu');
    menu.classList.toggle('dropdown-menu-visible');
}

window.onclick = function(event) {
    var 
        target = event.target,
        menu = document.getElementById('dropdown-menu')
    ;

    if(menu !== null) {
        if(!target.classList.contains('dropdown-toggle')) {
            menu.classList.remove('dropdown-menu-visible');
        }
    }

}

function toggleAriaAttribute(el) {
    var state = el.getAttribute('aria-expanded');
    // toggle state
    state = (state === 'true') ? 'false' : 'true';
    el.setAttribute('aria-expanded', state);
}

function toggleAccordion(el) {
    toggleAriaAttribute(el)
    // show/hide list
    el.nextElementSibling.classList.toggle('show');
}


document.addEventListener('DOMContentLoaded', function(){
    // hamburger
    var hamburger = document.getElementById('mobileNavToggle');
    var docsMobileButton = document.getElementById('mobileDocsNavToggle');
    var docsNav = document.getElementById('docsNav');

    hamburger.addEventListener('click', function() {
        mobileNavToggle();
    });

    if(docsMobileButton !== null) {
        docsMobileButton.addEventListener('click', function() {
        toggleAriaAttribute(docsMobileButton);
        docsMobileButton.classList.toggle('side-nav-visible');
        docsNav.classList.toggle('show');
        });
    }

    // accordion
    var collapsible = document.getElementsByClassName('collapse-trigger');
    
    if(collapsible.length) {
        for (var i = 0; i < collapsible.length; i++) {
            collapsible[i].addEventListener('click', function() {
                toggleAccordion(this);
            });
        }
        
        // open accordion for active doc section
        var active = document.querySelectorAll('.collapse .active');
        
        if(active.length) {
            var activeToggle = active[0].closest('.collapse').previousElementSibling
            toggleAccordion(activeToggle);
            if (activeToggle.closest('.collapse')) {
              toggleAccordion(activeToggle.closest('.collapse').previousElementSibling);
            }
        } else {
            toggleAccordion(document.querySelector('.collapse-trigger'));
            document.querySelector('.collapse a').classList.add('active');
        }
    }

    var dropdown = document.getElementById('dropdownMenuButton');
    
    if(dropdown !== null) {
        dropdown.addEventListener('click', function() {
            docsVersionToggle();
        });
    }
});
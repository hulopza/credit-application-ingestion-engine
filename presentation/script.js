document.addEventListener('DOMContentLoaded', () => {
  const slides = document.querySelectorAll('.slide');
  const dotsContainer = document.querySelector('.progress-indicator');
  const progressBar = document.querySelector('.bottom-progress-bar');
  const prevBtn = document.getElementById('prev-btn');
  const nextBtn = document.getElementById('next-btn');
  const playBtn = document.getElementById('play-btn');
  const autoAdvanceFill = document.querySelector('.auto-advance-fill');
  
  let currentSlide = 0;
  let isPlaying = false;
  let playInterval = null;
  let progressInterval = null;
  const slideDuration = 8000; // 8 seconds per slide in autoplay
  let startTime = 0;
  let pausedTime = 0;
  
  // Create progress dots dynamically
  slides.forEach((_, idx) => {
    const dot = document.createElement('div');
    dot.classList.add('progress-dot');
    if (idx === 0) dot.classList.add('active');
    dot.addEventListener('click', () => goToSlide(idx));
    dotsContainer.appendChild(dot);
  });
  
  const dots = document.querySelectorAll('.progress-dot');
  
  function updateSlideClasses() {
    slides.forEach((slide, idx) => {
      slide.classList.remove('active', 'prev');
      if (idx === currentSlide) {
        slide.classList.add('active');
      } else if (idx === currentSlide - 1 || (currentSlide === 0 && idx === slides.length - 1)) {
        slide.classList.add('prev');
      }
    });
    
    // Update dots
    dots.forEach((dot, idx) => {
      dot.classList.toggle('active', idx === currentSlide);
    });
    
    // Update bottom progress bar width
    const percentage = ((currentSlide) / (slides.length - 1)) * 100;
    progressBar.style.width = `${percentage}%`;
    
    // Update body theme class
    document.body.className = ''; // Reset
    const activeSlide = slides[currentSlide];
    const theme = activeSlide.dataset.theme;
    if (theme) {
      document.body.classList.add(theme);
    }
    
    // Reset auto advance timeline bar
    if (isPlaying) {
      startAutoplayTimer();
    } else {
      autoAdvanceFill.style.width = '0%';
    }
  }
  
  function goToSlide(index) {
    if (index < 0) {
      currentSlide = slides.length - 1;
    } else if (index >= slides.length) {
      currentSlide = 0;
    } else {
      currentSlide = index;
    }
    updateSlideClasses();
  }
  
  function nextSlide() {
    goToSlide(currentSlide + 1);
  }
  
  function prevSlide() {
    goToSlide(currentSlide - 1);
  }
  
  // Keyboard Navigation
  document.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowRight' || e.key === 'Space' || e.key === ' ') {
      e.preventDefault();
      nextSlide();
    } else if (e.key === 'ArrowLeft' || e.key === 'Backspace') {
      e.preventDefault();
      prevSlide();
    }
  });
  
  // Buttons navigation
  if (prevBtn) prevBtn.addEventListener('click', prevSlide);
  if (nextBtn) nextBtn.addEventListener('click', nextSlide);
  
  // Autoplay Logic
  function startAutoplayTimer() {
    clearInterval(playInterval);
    clearInterval(progressInterval);
    
    autoAdvanceFill.style.width = '0%';
    startTime = Date.now();
    
    progressInterval = setInterval(() => {
      const elapsed = Date.now() - startTime;
      const pct = Math.min((elapsed / slideDuration) * 100, 100);
      autoAdvanceFill.style.width = `${pct}%`;
    }, 50);
    
    playInterval = setInterval(() => {
      nextSlide();
    }, slideDuration);
  }
  
  function stopAutoplayTimer() {
    clearInterval(playInterval);
    clearInterval(progressInterval);
    autoAdvanceFill.style.width = '0%';
  }
  
  if (playBtn) {
    playBtn.addEventListener('click', () => {
      isPlaying = !isPlaying;
      if (isPlaying) {
        playBtn.innerHTML = '⏸';
        playBtn.title = 'Pause Slideshow';
        startAutoplayTimer();
      } else {
        playBtn.innerHTML = '▶';
        playBtn.title = 'Play Slideshow';
        stopAutoplayTimer();
      }
    });
  }
  
  // Custom slide-specific interactions
  // Hook up interactive node click highlighting on Slide 2/3
  document.querySelectorAll('.diagram-node').forEach(node => {
    node.addEventListener('click', () => {
      // Toggle highlight
      const wasHighlighted = node.classList.contains('highlighted');
      document.querySelectorAll('.diagram-node').forEach(n => n.classList.remove('highlighted'));
      
      if (!wasHighlighted) {
        node.classList.add('highlighted');
        // If there's metadata detail card associated, highlight it
        const targetId = node.dataset.target;
        if (targetId) {
          const detailCard = document.getElementById(targetId);
          if (detailCard) {
            detailCard.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
            detailCard.style.borderColor = 'var(--theme-color)';
            detailCard.style.boxShadow = '0 0 20px rgba(var(--theme-color-rgb), 0.2)';
            setTimeout(() => {
              detailCard.style.borderColor = '';
              detailCard.style.boxShadow = '';
            }, 2500);
          }
        }
      }
    });
  });

  // Initialize
  updateSlideClasses();
});

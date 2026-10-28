/* Canal Rust — three.js "data canal" hero scene */
import * as THREE from '../vendor/three.module.min.js';

const host = document.getElementById('canal-canvas');
if (host) {
  const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  try {
    const renderer = new THREE.WebGLRenderer({
      alpha: true,
      antialias: true,
      powerPreference: 'high-performance',
    });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.5));
    renderer.setClearColor(0x000000, 0);
    renderer.domElement.setAttribute('aria-hidden', 'true');
    host.appendChild(renderer.domElement);

    const scene = new THREE.Scene();
    const camera = new THREE.PerspectiveCamera(42, 1, 0.1, 60);
    camera.position.set(0, 0.4, 14);
    camera.lookAt(0, 0, 0);

    /* Soft round sprite used by both stream points and event orbs. */
    const glowCanvas = document.createElement('canvas');
    glowCanvas.width = glowCanvas.height = 128;
    const ctx = glowCanvas.getContext('2d');
    const gradient = ctx.createRadialGradient(64, 64, 0, 64, 64, 64);
    gradient.addColorStop(0, 'rgba(255,255,255,1)');
    gradient.addColorStop(0.28, 'rgba(255,255,255,0.85)');
    gradient.addColorStop(0.62, 'rgba(255,255,255,0.22)');
    gradient.addColorStop(1, 'rgba(255,255,255,0)');
    ctx.fillStyle = gradient;
    ctx.fillRect(0, 0, 128, 128);
    const glow = new THREE.CanvasTexture(glowCanvas);
    glow.minFilter = THREE.LinearFilter;

    /* A stream of binlog events drifting down the canal. */
    const wide = host.clientWidth > 640;
    const count = wide ? 320 : 150;
    const positions = new Float32Array(count * 3);
    const colors = new Float32Array(count * 3);
    const speeds = new Float32Array(count);
    const palette = ['#63d6f2', '#2a93cc', '#e8552b', '#f3b44a', '#9bebff'];
    const color = new THREE.Color();

    for (let i = 0; i < count; i += 1) {
      positions[i * 3] = (Math.random() - 0.5) * 22;
      positions[i * 3 + 1] = (Math.random() - 0.5) * 9;
      positions[i * 3 + 2] = -8 + Math.random() * 10;
      speeds[i] = 0.008 + Math.random() * 0.035;
      color.set(palette[Math.floor(Math.random() * palette.length)]);
      colors[i * 3] = color.r;
      colors[i * 3 + 1] = color.g;
      colors[i * 3 + 2] = color.b;
    }

    const streamGeometry = new THREE.BufferGeometry();
    streamGeometry.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    streamGeometry.setAttribute('color', new THREE.BufferAttribute(colors, 3));
    const stream = new THREE.Points(
      streamGeometry,
      new THREE.PointsMaterial({
        size: 0.15,
        map: glow,
        vertexColors: true,
        transparent: true,
        opacity: 0.9,
        depthWrite: false,
        blending: THREE.AdditiveBlending,
        sizeAttenuation: true,
      }),
    );
    scene.add(stream);

    /* A handful of brighter packets, each a discrete change event. */
    const orbs = [];
    const orbColors = ['#9bebff', '#ffb27f', '#f3b44a', '#63d6f2'];
    for (let i = 0; i < 11; i += 1) {
      const sprite = new THREE.Sprite(
        new THREE.SpriteMaterial({
          map: glow,
          color: orbColors[i % orbColors.length],
          transparent: true,
          opacity: 0.9,
          depthWrite: false,
          blending: THREE.AdditiveBlending,
        }),
      );
      sprite.position.set((Math.random() - 0.5) * 20, (Math.random() - 0.5) * 7.5, -2 - Math.random() * 4);
      const scale = 0.28 + Math.random() * 0.34;
      sprite.scale.set(scale, scale, 1);
      sprite.userData.speed = 0.02 + Math.random() * 0.045;
      orbs.push(sprite);
      scene.add(sprite);
    }

    /* Wireframe gate wheel behind the mascot. */
    const wheel = new THREE.Group();
    const ringMaterial = new THREE.MeshBasicMaterial({
      color: 0xe8552b,
      transparent: true,
      opacity: 0.3,
      depthWrite: false,
    });
    const ring = new THREE.Mesh(new THREE.TorusGeometry(4.7, 0.016, 6, 160), ringMaterial);
    const innerRing = new THREE.Mesh(
      new THREE.TorusGeometry(3.35, 0.01, 6, 120),
      new THREE.MeshBasicMaterial({
        color: 0x63d6f2,
        transparent: true,
        opacity: 0.16,
        depthWrite: false,
      }),
    );
    wheel.add(ring, innerRing);

    const spokePositions = [];
    for (let i = 0; i < 8; i += 1) {
      const angle = (i / 8) * Math.PI * 2;
      spokePositions.push(
        Math.cos(angle) * 1.35, Math.sin(angle) * 1.35, 0,
        Math.cos(angle) * 4.7, Math.sin(angle) * 4.7, 0,
      );
    }
    const spokes = new THREE.LineSegments(
      new THREE.BufferGeometry().setAttribute(
        'position',
        new THREE.Float32BufferAttribute(spokePositions, 3),
      ),
      new THREE.LineBasicMaterial({
        color: 0xe8552b,
        transparent: true,
        opacity: 0.17,
        depthWrite: false,
      }),
    );
    wheel.add(spokes);
    wheel.rotation.x = 1.1;
    wheel.rotation.z = -0.12;
    scene.add(wheel);

    let running = true;
    let frame = 0;
    let pointerX = 0;
    let pointerY = 0;
    let targetX = 0;
    let targetY = 0;
    const clock = new THREE.Clock();

    function size() {
      const width = host.clientWidth || 1;
      const height = host.clientHeight || 1;
      renderer.setSize(width, height, false);
      renderer.domElement.style.width = '100%';
      renderer.domElement.style.height = '100%';
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
      if (reduceMotion) renderer.render(scene, camera);
    }

    function animate() {
      if (!running || document.hidden) {
        frame = 0;
        return;
      }
      frame = requestAnimationFrame(animate);
      const time = clock.getElapsedTime();

      const pos = streamGeometry.attributes.position.array;
      for (let i = 0; i < count; i += 1) {
        pos[i * 3] += speeds[i];
        if (pos[i * 3] > 11) {
          pos[i * 3] = -11;
          pos[i * 3 + 1] = (Math.random() - 0.5) * 9;
        }
      }
      streamGeometry.attributes.position.needsUpdate = true;

      orbs.forEach((orb) => {
        orb.position.x += orb.userData.speed;
        if (orb.position.x > 11) {
          orb.position.x = -11;
          orb.position.y = (Math.random() - 0.5) * 7.5;
        }
        const pulse = 1 + Math.sin(time * 2 + orb.position.x) * 0.16;
        orb.scale.setScalar((orb.userData.speed * 13 + 0.22) * pulse);
      });

      wheel.rotation.z = time * 0.055 - 0.12;
      wheel.rotation.x = 1.1 + Math.sin(time * 0.22) * 0.05;

      targetX = pointerX * 0.55;
      targetY = pointerY * 0.32;
      camera.position.x += (targetX - camera.position.x) * 0.045;
      camera.position.y += (0.4 + targetY - camera.position.y) * 0.045;
      camera.lookAt(0, 0, 0);

      renderer.render(scene, camera);
    }

    function start() {
      if (reduceMotion || frame) return;
      clock.start();
      frame = requestAnimationFrame(animate);
    }

    function stop() {
      if (frame) cancelAnimationFrame(frame);
      frame = 0;
    }

    host.addEventListener('pointermove', (event) => {
      const rect = host.getBoundingClientRect();
      pointerX = ((event.clientX - rect.left) / rect.width - 0.5) * 2;
      pointerY = ((event.clientY - rect.top) / rect.height - 0.5) * 2;
    });
    host.addEventListener('pointerleave', () => {
      pointerX = 0;
      pointerY = 0;
    });

    if ('ResizeObserver' in window) {
      new ResizeObserver(size).observe(host);
    } else {
      window.addEventListener('resize', size);
    }

    if ('IntersectionObserver' in window) {
      new IntersectionObserver((entries) => {
        running = entries[0].isIntersecting;
        if (running) start(); else stop();
      }, { threshold: 0.01 }).observe(host);
    }

    document.addEventListener('visibilitychange', () => {
      if (document.hidden) stop(); else start();
    });

    size();
    if (reduceMotion) {
      renderer.render(scene, camera);
    } else {
      start();
    }
  } catch (error) {
    /* No WebGL: the CSS gradient and pet remain the hero. */
    host.classList.add('no-webgl');
  }
}

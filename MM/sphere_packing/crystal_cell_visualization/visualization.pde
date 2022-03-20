import java.util.Scanner; //<>//
import java.io.FileReader;
import java.util.Locale;

class Atom {
  final String element;
  final PVector position;
  final float radius;
  final color clr;

  public Atom(PVector position, float radius, color clr, String element) {
    this.position = position;
    this.radius = radius;
    this.clr = clr;
    this.element = element;
  }

  void draw() {
    pushMatrix();
    fill(clr);
    translate(position.x, position.y, position.z);
    sphere(radius);
    popMatrix();
  }
}

color generateColor(int totalColors, int iColor) {
  colorMode(HSB, 255, 255, 255);
  color clr = color((int)(255.0 * (float) iColor / totalColors), 255, 255);
  colorMode(RGB, 255, 255, 255);
  return clr;
}

float a;
ArrayList<Atom> atoms;

float viewA = 100.0;

class ColorWrapper {
  final color clr;
  ColorWrapper(color clr) {
    this.clr = clr;
  }

  color get() {
    return clr;
  }
}

ArrayList<ColorWrapper> elementColor = new ArrayList<ColorWrapper>();
ArrayList<String> elementName = new ArrayList<String>();

ArrayList<Atom> loadAtoms(String path) {
  ArrayList<Atom> result = new ArrayList<Atom>();
  try(Scanner scanner = new Scanner(new FileReader(path))) {
    a = (float)scanner.nextDouble();
    int distinctAtomTypeCount = scanner.nextInt();
    for (int i = 0; i < distinctAtomTypeCount; ++i) {
      String atomElementName = scanner.next();
      float atomRadius = (float)scanner.nextDouble();
      int atomsCount = scanner.nextInt();

      color clr = generateColor(distinctAtomTypeCount, i);

      elementColor.add(new ColorWrapper(clr));
      elementName.add(atomElementName);

      for (int j = 0; j < atomsCount; ++j) {
        float x = ((float)scanner.nextDouble() - 0.5);
        float y = ((float)scanner.nextDouble() - 0.5);
        float z = ((float)scanner.nextDouble() - 0.5);

        x *= viewA;
        y *= viewA;
        z *= viewA;

        result.add(new Atom(new PVector(x, y, z), atomRadius * viewA / a, clr, atomElementName));
      }
    }
  }
  catch(IOException ex) {
    System.out.println(ex.getMessage());
    return null;
  }
  return result;
}

PVector[] positions;

void setup() {
  size(800, 600, P3D);
  smooth(8);

  Locale.setDefault(new Locale("en", "US"));
  atoms = loadAtoms(sketchPath() + "/../result.txt");
}

float scaleFactor = 1.0;

void drawCellGrid() {
  strokeWeight(1.5 / scaleFactor);
  stroke(0);
  line(-viewA/2, -viewA/2, -viewA/2, viewA/2, -viewA/2, -viewA/2);
  line(-viewA/2, -viewA/2, -viewA/2, -viewA/2, viewA/2, -viewA/2);
  line(-viewA/2, -viewA/2, -viewA/2, -viewA/2, -viewA/2, viewA/2);

  line(-viewA/2, viewA/2, viewA/2, -viewA/2, -viewA/2, viewA/2);
  line(-viewA/2, viewA/2, viewA/2, -viewA/2, viewA/2, -viewA/2);

  line(viewA/2, -viewA/2, -viewA/2, viewA/2, viewA/2, -viewA/2);
  line(viewA/2, -viewA/2, -viewA/2, viewA/2, -viewA/2, viewA/2);

  line(-viewA/2, -viewA/2, viewA/2, viewA/2, -viewA/2, viewA/2);
  line(-viewA/2, viewA/2, -viewA/2, viewA/2, viewA/2, -viewA/2);

  line(viewA/2, viewA/2, viewA/2, -viewA/2, viewA/2, viewA/2);
  line(viewA/2, viewA/2, viewA/2, viewA/2, -viewA/2, viewA/2);
  line(viewA/2, viewA/2, viewA/2, viewA/2, viewA/2, -viewA/2);
}

void mouseWheel(MouseEvent e) {
  scaleFactor += e.getCount() * 0.05;
  scaleFactor = constrain(scaleFactor, 0.1, 10.0);
}

float angleX = 0.0;
float angleY = 0.0;

float prevX = 0.0;
float prevY = 0.0;

void mousePressed() {
  prevX = mouseX;
  prevY = mouseY;
}

void mouseDragged() {
  angleY += (mouseX-prevX);
  angleX += -(mouseY-prevY);
  prevX = mouseX;
  prevY = mouseY;
}

void draw() {
  background(120);

  for (int i = 0; i < elementColor.size(); ++i) {
    rectMode(CORNER);
    fill(elementColor.get(i).get());
    rect(width / 100.0, (6.0 * (i + 1) * height) / 100.0, 20, 20);
    textAlign(LEFT, TOP);
    textSize(20.0);
    text(elementName.get(i), (4.0 * width) / 100.0, (6.0 * (i + 1) * height) / 100.0);
  }

  ortho();

  lights();

  translate(width / 2, height / 2);
  scale(scaleFactor);
  rotateX(angleX / height * PI);
  rotateY(angleY / width * TWO_PI);

  drawCellGrid();

  noStroke();

  for (var a : atoms) {
    a.draw();
  }
}
